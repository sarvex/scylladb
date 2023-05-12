#!/usr/bin/env python3

import os
import glob
import sys
import time
import shutil
import signal
import atexit
import tempfile
import requests

# run_with_temporary_dir() is a utility function for running a process, such
# as Scylla, Cassandra or Redis, inside its own new temporary directory,
# and ensure that on exit for any reason - success, failure, signal or
# exception - the subprocess is killed and its temporary directory is deleted.
#
# The parameter to run_with_temporary_dir() is a function which receives the
# new process id and the new temporary directory's path, and builds the
# command line to run and map of extra environment variables. This function
# can put files in the directory (which already exists when it is called).
# See below the example run_scylla_cmd.

def pid_to_dir(pid):
    return os.path.join(os.getenv('TMPDIR', '/tmp'), f'scylla-test-{str(pid)}')

def run_with_generated_dir(run_cmd_generator, run_dir_generator):
    global run_with_temporary_dir_pids
    global run_pytest_pids
    # Below, there is a small time window, after we fork and the child
    # started running but before we save this child's process id in
    # run_with_temporary_dir_pids. In that small time window, a signal may
    # kill the parent process but not cleanup the child. So we use sigmask
    # to postpone signal delivery during that time window:
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, {})
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGQUIT, signal.SIGTERM})
    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:
        # Child
        run_with_temporary_dir_pids = set() # no children to clean up on child
        run_pytest_pids = set()
        pid = os.getpid()
        dir = run_dir_generator(pid)
        (cmd, env) = run_cmd_generator(pid, dir)
        # redirect stdout and stderr to log file, as in a shell's >log 2>&1:
        log = os.path.join(dir, 'log')
        fd = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, mode=0o666)
        sys.stdout.flush()
        os.close(1)
        os.dup2(fd, 1)
        sys.stderr.flush()
        os.close(2)
        os.dup2(fd, 2)
        # Detach child from parent's "session", so that a SIGINT will be
        # delivered just to the parent, not to the child. Instead, the parent
        # will eventually deliver a SIGKILL as part of cleanup_all().
        os.setsid()
        os.execve(cmd[0], cmd, dict(os.environ, **env))
        # execve will not return. If it cannot run the program, it will raise
        # an exception.
    # Parent
    run_with_temporary_dir_pids.add(pid)
    signal.pthread_sigmask(signal.SIG_SETMASK, mask)
    return pid

def make_new_tempdir(pid):
    dir = pid_to_dir(pid)
    os.mkdir(dir)
    return dir

def run_with_temporary_dir(run_cmd_generator):
    return run_with_generated_dir(run_cmd_generator, make_new_tempdir)

def restart_with_dir(old_pid, run_cmd_generator, dir):
    try:
        os.killpg(old_pid, 2)
        os.waitpid(old_pid, 0)
    except ProcessLookupError:
        pass

    scylla_link = os.path.join(dir, 'test_scylla')
    os.unlink(scylla_link)
    return run_with_generated_dir(run_cmd_generator, lambda pid : dir)

# run_with_temporary_dir_pids is a set of process ids previously created
# by run_with_temporary_dir(). On exit, the processes listed here are
# cleaned up. Note that there is a known mapping (pid_to_dir()) from each
# pid to the temporary directory - which will also be removed.
run_with_temporary_dir_pids = set()

# abort_run_with_temporary_dir() kills a process started earlier by
# run_with_temporary_directory, and and removes its temporary directory.
# Currently, the log file is opened and returned so the caller can show
# it to the standard output even after the directory is removed. In the
# future we may want to change this - and save the log somewhere instead
# of copying it to stdout.
def abort_run_with_dir(pid, tmpdir):
    try:
        os.killpg(pid, 9)
        os.waitpid(pid, 0) # don't leave an annoying zombie
    except ProcessLookupError:
        pass
    # We want to read tmpdir/log to stdout, but if stdout is piped, this can
    # take a long time and be interrupted. We don't want the rmtree() below
    # to not happen in that case. So we need to open the log file first,
    # delete the directory (the open file will not be really deleted unti we
    # close it) - and only then start showing the log file.
    f = open(os.path.join(tmpdir, 'log'), 'rb')
    # Be paranoid about rmtree accidentally removing the entire disk...
    # TODO: check tmpdir is actually in TMPDIR and refuse to remove it
    # if not.
    if tmpdir != '/':
        shutil.rmtree(tmpdir)
    return f

def abort_run_with_temporary_dir(pid):
    return abort_run_with_dir(pid, pid_to_dir(pid))

summary=''
run_pytest_pids = set()

def cleanup_all():
    global summary
    global run_with_temporary_dir_pids
    global run_pytest_pids
    # Kill pytest first, before killing the tested server, so we don't
    # continue to get a barrage of errors when the test runs with the
    # server killed.
    for pid in run_pytest_pids:
        try:
            os.killpg(pid, 9)
            os.waitpid(pid, 0) # don't leave an annoying zombie
        except ProcessLookupError:
            pass
    for pid in run_with_temporary_dir_pids:
        f = abort_run_with_temporary_dir(pid)
        print('\nSubprocess output:\n')
        sys.stdout.flush()
        shutil.copyfileobj(f, sys.stdout.buffer)
    scylla_set = set()
    print(summary)

# We run the cleanup_all() function on exit for any reason - successful finish
# of the script, an uncaught exception, or a signal. It ensures that the
# subprocesses (e.g., Scylla) is killed and its temporary storage directory
# is deleted. It also shows the subprocesses's output log.
atexit.register(cleanup_all)

# If Python doesn't catch a particular signal, the atexit handler will not
# get called. By default SIGINT is caught, but SIGTERM and SIGHUP are not,
# so let's catch them explicitly:
for sig in [signal.SIGTERM, signal.SIGHUP]:
    signal.signal(sig, lambda sig, frame:
        sys.exit(f'Received signal {signal.Signals(sig).name}. Exiting.'))

##############################

# When we run a server - e.g., Scylla, Cassandra, or Redis - we want to
# have it listen on a unique IP address so it doesn't collide with other
# servers run by other concurrent tests. Luckily, Linux allows us to use any
# IP address in the range 127/8 (i.e., 127.*.*.*). If we pick an IP address
# based on the server's main process id, we know two servers will not
# get the same IP address. We avoid 127.0.*.* because CCM (a different test
# framework) assumes it will be available for it to run Scylla instances.
# 127.255.255.255 is also illegal. So we use 127.{1-254}.*.*.
# This gives us a total of 253*255*255 possible IP addresses - which is
# significantly more than /proc/sys/kernel/pid_max on any system I know.
def pid_to_ip(pid):
    bytes = pid.to_bytes(3, byteorder='big')
    return f'127.{str(bytes[0] + 1)}.{str(bytes[1])}.{str(bytes[2])}'

##############################

# Specific code for running *Scylla*:

import cassandra.cluster
import ssl

# Find a Scylla executable. By default, we take the latest build/*/scylla
# next to the location of this script, but this can be overridden by setting
# a SCYLLA environment variable:
source_path = os.path.realpath(os.path.join(__file__, '../../..'))
scylla = None
def find_scylla():
    global scylla
    global source_path
    if scylla:
        return scylla
    if os.getenv('SCYLLA'):
        scylla = os.path.abspath(os.getenv('SCYLLA'))
    else:
        scyllas = glob.glob(os.path.join(source_path, 'build/*/scylla'))
        if not scyllas:
            print(
                f"Can't find a Scylla executable in {source_path}.\nPlease build Scylla or set SCYLLA to the path of a Scylla executable."
            )
            exit(1)
        scylla = max(scyllas, key=os.path.getmtime)
    if not os.access(scylla, os.X_OK):
        print(
            f"Cannot execute '{scylla}'.\nPlease set SCYLLA to the path of a Scylla executable."
        )
        exit(1)
    return scylla

def run_scylla_cmd(pid, dir):
    ip = pid_to_ip(pid)
    print(f'Booting Scylla on {ip} in {dir}...')
    global scylla
    global source_path
    # To make things easier for users of "killall", "top", and similar,
    # we want the Scylla executable which we run during the test to have
    # a different name from manual runs of Scylla. Unfortunately, using
    # execve() to change just argv[0] isn't good enough - because killall
    # inspects the actual executable filename in /proc/<pid>/stat. So we
    # need to name the executable differently. Luckily, using a symbolic
    # link is good enough.
    scylla_link = os.path.join(dir, 'test_scylla')
    os.symlink(scylla, scylla_link)
    # When running a Scylla build with sanitizers enabled, we should
    # configure them to fail on real errors, and ignore spurious errors.
    env = {
        'UBSAN_OPTIONS': f'halt_on_error=1:abort_on_error=1:suppressions={source_path}/ubsan-suppressions.supp',
        'ASAN_OPTIONS': 'disable_coredump=0:abort_on_error=1:detect_stack_use_after_returns=1'
    }
    return [
        scylla_link,
        '--options-file',
        f'{source_path}/conf/scylla.yaml',
        '--developer-mode',
        '1',
        '--ring-delay-ms',
        '0',
        '--collectd',
        '0',
        '--smp',
        '2',
        '-m',
        '1G',
        '--overprovisioned',
        '--max-networking-io-control-blocks',
        '1000',
        '--unsafe-bypass-fsync',
        '1',
        '--kernel-page-cache',
        '1',
        '--commitlog-use-o-dsync',
        '0',
        '--flush-schema-tables-after-modification',
        'false',
        '--api-address',
        ip,
        '--rpc-address',
        ip,
        '--listen-address',
        ip,
        '--prometheus-address',
        ip,
        '--seed-provider-parameters',
        f'seeds={ip}',
        '--workdir',
        dir,
        '--auto-snapshot',
        '0',
        '--skip-wait-for-gossip-to-settle',
        '0',
        '--logger-log-level',
        'compaction=warn',
        '--logger-log-level',
        'migration_manager=warn',
        '--num-tokens',
        '16',
        '--query-tombstone-page-limit',
        '1000',
        '--range-request-timeout-in-ms',
        '300000',
        '--read-request-timeout-in-ms',
        '300000',
        '--counter-write-request-timeout-in-ms',
        '300000',
        '--cas-contention-timeout-in-ms',
        '300000',
        '--truncate-request-timeout-in-ms',
        '300000',
        '--write-request-timeout-in-ms',
        '300000',
        '--request-timeout-in-ms',
        '300000',
        '--experimental-features=udf',
        '--experimental-features=keyspace-storage-options',
        '--enable-user-defined-functions',
        '1',
        '--authenticator',
        'PasswordAuthenticator',
        '--authorizer',
        'CassandraAuthorizer',
        '--strict-allow-filtering',
        'true',
        '--permissions-update-interval-in-ms',
        '100',
        '--permissions-validity-in-ms',
        '100',
    ], env

# Same as run_scylla_cmd, just use SSL encryption for the CQL port (same
# port number as default - replacing the unencrypted server)
def run_scylla_ssl_cql_cmd(pid, dir):
    (cmd, env) = run_scylla_cmd(pid, dir)
    setup_ssl_certificate(dir)
    cmd += ['--client-encryption-options', 'enabled=true',
            '--client-encryption-options', f'keyfile={dir}/scylla.key',
            '--client-encryption-options', f'certificate={dir}/scylla.crt',
    ]
    return (cmd, env)

# Get a Cluster object to connect to CQL at the given IP address (and with
# the appropriate username and password). It's important to shutdown() this
# Cluster object when done with it, otherwise we can get errors at the end
# of the run when background tasks continue to spawn futures after exit.
def get_cql_cluster(ip, ssl_context=None):
    auth_provider = cassandra.auth.PlainTextAuthProvider(username='cassandra', password='cassandra')
    return cassandra.cluster.Cluster([ip],
        auth_provider=auth_provider,
        ssl_context=ssl_context,
        # The default timeout for new connections is 5 seconds, and for
        # requests made by the control connection is 2 seconds. These should
        # have been more than enough, but in some extreme cases with a very
        # slow debug build running on a very busy machine, they may not be.
        # so let's increase them to 60 seconds. See issue #13239.
        connect_timeout = 60,
        control_connection_timeout = 60)

## Test that CQL is serving, for wait_for_services() below.
def check_cql(ip, ssl_context=None):
    try:
        cluster = get_cql_cluster(ip, ssl_context)
        cluster.connect()
        cluster.shutdown()
    except cassandra.cluster.NoHostAvailable:
        raise NotYetUp
    # Any other exception may indicate a problem, and is passed to the caller.
def check_ssl_cql(ip):
    # Note that Scylla does not support any earlier TLS protocol. If you
    # try, you get mysterious EOF errors (see issue #6971) :-(
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    check_cql(ip, ssl_context)

# Test that the Scylla REST API is serving.
# Can be used as a checker function with wait_for_services() below.
def check_rest_api(ip, port=10000):
    try:
        requests.get(f"http://{ip}:{port}/")
        # getting "/" returns error status 404 but we don't care
        # as long as the server returns it
    except requests.exceptions.ConnectionError:
        raise NotYetUp
    # Any other exception may indicate a problem, and is passed to the caller.

# wait_for_services() waits for scylla to finish booting successfully and
# listen to services checked by the given "checkers". Raises an exception
# if we know Scylla did not boot properly (as soon as we know - not waiting
# for a timeout).
#
# Each checker is a function which returns successfully if the service it
# checks is up, or throws an exception if it is not. If the service is not
# *yet* up, it should throw the NotYetUp exception, indicating that
# wait_for_services() should continue to retry. Any other exceptions means
# an unrecoverable error was detected, and retry would be hopeless.
#
# wait_for_services has a hard-coded timeout of 200 seconds.
class NotYetUp(Exception):
    pass
def wait_for_services(pid, checkers):
    start_time = time.time()
    ready = False
    while time.time() < start_time + 200:
        time.sleep(0.1)
        # To check if Scylla died already (i.e., failed to boot), we need
        # to first get rid of the zombie (if it exists) with waitpid, and
        # then check if the process still exists, with kill.
        try:
            os.waitpid(pid, os.WNOHANG)
            os.kill(pid, 0)
        except (ProcessLookupError, ChildProcessError):
            # Scylla is dead, we cannot recover
            break
        try:
            for checker in checkers:
                checker()
            # If all checkers passed, we're finally done
            ready = True
            break
        except NotYetUp:
            pass
    duration = f'{str(round(time.time() - start_time, 1))} seconds'
    if not ready:
        print(f'Boot failed after {duration}.')
        # Run the checkers again, not catching NotYetUp, to show exception
        # traces of which of the checks failed and how.
        os.waitpid(pid, os.WNOHANG)
        os.kill(pid, 0)
        for checker in checkers:
            checker()
    print(f'Boot successful ({duration}).')
    sys.stdout.flush()

def wait_for_cql(pid, ip):
    wait_for_services(pid, [lambda: check_cql(ip)])

def run_pytest(pytest_dir, additional_parameters):
    global run_with_temporary_dir_pids
    global run_pytest_pids
    sys.stdout.flush()
    sys.stderr.flush()
    pid = os.fork()
    if pid == 0:
        # child:
        run_with_temporary_dir_pids = set() # no children to clean up on child
        run_pytest_pids = set()
        os.chdir(pytest_dir)
        os.setsid()
        os.execvp('pytest', ['pytest',
            '-o', 'junit_family=xunit2'] + additional_parameters)
        exit(1)
    # parent:
    run_pytest_pids.add(pid)
    return not os.waitpid(pid, 0)[1]

# Set up self-signed SSL certificate in dir/scylla.key, dir/scylla.crt.
# These can be used for setting up an HTTPS server for Alternator, or for
# any other part of Scylla which needs SSL.
def setup_ssl_certificate(dir):
    # FIXME: error checking (if "openssl" isn't found, for example)
    os.system(f'openssl genrsa 2048 > "{dir}/scylla.key"')
    os.system(f'openssl req -new -x509 -nodes -sha256 -days 365 -subj "/C=IL/ST=None/L=None/O=None/OU=None/CN=example.com" -key "{dir}/scylla.key" -out "{dir}/scylla.crt"')
