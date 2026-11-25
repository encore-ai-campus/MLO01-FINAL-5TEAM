"""Shared Soda contract checks and Delta audit schema."""
from threading import Event, Thread
from time import perf_counter

import psutil
import yaml
from soda_core.contracts import verify_contract_locally


AUDIT_SCHEMA = (
    'case string, attempt int, status string, evaluated_checks int, passed_checks int, '
    'failed_checks int, elapsed_seconds double, peak_process_tree_rss_bytes long, '
    'alert_requested boolean, downstream_allowed boolean, error_summary string'
)


def required_checks(contract):
    definition = yaml.safe_load(contract)
    return {check['name'] for item in [definition, *definition['columns']]
            for entry in item.get('checks', []) for check in entry.values()}


def peak_process_tree(done, measurements):
    root = psutil.Process()
    while not done.is_set():
        rss = 0
        for process in [root, *root.children(recursive=True)]:
            try:
                rss += process.memory_info().rss
            except psutil.NoSuchProcess:
                continue
        measurements.append(rss)
        done.wait(.05)


def evaluate(source, path, required):
    samples = []
    done = Event()
    sampler = Thread(target=peak_process_tree, args=(done, samples))
    started = perf_counter()
    sampler.start()
    try:
        result = verify_contract_locally(data_sources=[source], contract_file_path=str(path), publish=False)
        errors = result.get_errors_str()
        checks = [check for item in result.contract_verification_results for check in item.check_results]
        failed = sum(check.is_failed for check in checks)
        passed = sum(check.is_passed for check in checks)
        if errors:
            status = 'execution_error'
        elif {check.check.name for check in checks} != required or len(checks) != len(required) or passed + failed != len(required):
            status = 'missing_check'
        elif not result.is_ok or failed:
            status = 'violation'
        else:
            status = 'pass'
    except Exception as error:
        status, checks, passed, failed, errors = 'execution_error', [], 0, 0, str(error)
    finally:
        duration = perf_counter() - started
        done.set()
        sampler.join()
    return status, len(checks), passed, failed, round(duration, 3), max(samples), errors or ''


def append_audit(spark, destination, case, attempt, result):
    status, evaluated, passed, failed, elapsed, memory, errors = result
    record = (case, attempt, status, evaluated, passed, failed, elapsed, memory,
              status != 'pass', status == 'pass', errors[:600])
    spark.createDataFrame([record], AUDIT_SCHEMA).write.format('delta').mode('append').save(str(destination))
    return record
