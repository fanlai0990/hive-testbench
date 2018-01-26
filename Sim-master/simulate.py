import argparse
from src.utils import formater
import logging.config
from time import gmtime, strftime
from shutil import copyfile
from src.scheduler import (RecursiveCoflowScheduler, RecursiveRemainScheduler, PoorManScheduler, RemainFlowScheduler,
                           CombineCoflowScheduler, ReverseRecursiveCoflowScheduler, MaxMinFlowScheduler)
from src.manager import SimulationManager, SparkEnableSimulationManager, BaselineSparkSimulationManager
import src.globals

RECURSIVE_COFLOW_SCHEDULER = 'recursive-coflow'
REVERSE_RECURSIVE_COFLOW_SCHEDULER = 'reverse-recursive-coflow'
REMAIN_FLOW_SCHEDULER = 'remain-flow'
RECURSIVE_REMAIN_FLOW_SCHEDULER = 'recursive-remain-flow'
COMBINE_COFLOW_SCHEDULER = 'combine-coflow'
BASELINE_SCHEDULER = 'baseline'
POORMANS_SCHEDULER = 'poor-man'

scheduler_dict = {
    RECURSIVE_COFLOW_SCHEDULER: RecursiveCoflowScheduler,
    REVERSE_RECURSIVE_COFLOW_SCHEDULER: ReverseRecursiveCoflowScheduler,
    REMAIN_FLOW_SCHEDULER: RemainFlowScheduler,
    RECURSIVE_REMAIN_FLOW_SCHEDULER: RecursiveRemainScheduler,
    COMBINE_COFLOW_SCHEDULER: CombineCoflowScheduler,
    BASELINE_SCHEDULER: MaxMinFlowScheduler,
    POORMANS_SCHEDULER: PoorManScheduler
}

wrapper = formater(width=100)


def setup_logging(logging_level, log_file):

    log_file = log_file or "logging/%s-debug.log" % strftime("%Y-%m-%d %H:%M:%S", gmtime())
    if logging_level is None:
        logging_level = logging.ERROR
    else:
        if logging_level == 1:
            logging_level = logging.INFO
        elif logging_level == 2:
            logging_level = logging.DEBUG
        elif logging_level == 3:
            logging_level = logging.ERROR
        else:
            logging_level = logging.NOTSET
    logging.basicConfig(level=logging_level, filename=log_file, disable_existing_loggers=False,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


def setup_argparse(p):
    p.add_argument('-g', '--gml', help=wrapper.wrap('Network data in gml format'), required=True)
    p.add_argument(
        '-s', '--scheduler',
        help=wrapper.wrap((
            'scheduler used. Default scheduler: recursive coflow scheduler [%s]. '
            'Supported scheduler: 1. recursive remain flow scheduler [%s], '
            '2. recursive coflow scheduler [%s], '
            '3. remain flow scheduler [%s], '
            '4. reverse recursive coflow scheduler [%s], '
            '5. combine coflow scheduler [%s], '
            '6. baseline flow scheduler use MaxMinFlow scheduler [%s]'
            ) % (RECURSIVE_REMAIN_FLOW_SCHEDULER, RECURSIVE_REMAIN_FLOW_SCHEDULER, RECURSIVE_COFLOW_SCHEDULER,
                 REMAIN_FLOW_SCHEDULER, REVERSE_RECURSIVE_COFLOW_SCHEDULER, COMBINE_COFLOW_SCHEDULER,
                 BASELINE_SCHEDULER)
        ),
        default=RECURSIVE_COFLOW_SCHEDULER
    )
    p.add_argument('-c', '--coflow', help=wrapper.wrap('path to coflow file if defined'), required=False)
    p.add_argument('-j', '--job', help=wrapper.wrap('path to job file if defined'), required=False)
    p.add_argument('-l', '--level', help=wrapper.wrap('log level'), required=False, type=int)
    p.add_argument('-lf', '--logfile', help=wrapper.wrap('log file to write the log'), required=False)


def create_manager(args):
    if args.coflow is not None:
        return SimulationManager(gml_path=args.gml,
                                coflow_path=args.coflow,
                                scheduler_class=scheduler_dict[args.scheduler]
                                )
    elif args.job is not None:
        if args.scheduler == BASELINE_SCHEDULER:
            # use baseline manager if try to scheduler baseline manager
            src.globals.isBaseline = True
            return BaselineSparkSimulationManager(gml_path=args.gml, spark_path=args.job)
        else:
            return SparkEnableSimulationManager(gml_path=args.gml,
                                            spark_path=args.job,
                                            scheduler_class=scheduler_dict[args.scheduler])
        # TODO: Now spark only supports for coflow level scheduler
    else:
        raise Exception("You need to either specify a --job path or a --coflow path")


def main():
    p = argparse.ArgumentParser(
        description=wrapper.wrap('Simulate coflow scheduling'),
        formatter_class=argparse.RawTextHelpFormatter
    )
    setup_argparse(p)
    args = p.parse_args()
    setup_logging(logging_level=args.level, log_file=args.logfile)
    copyfile("models/MinCCT.mod", "/tmp/MinCCT.mod")
    manager = create_manager(args)
    manager.simulate()

if __name__ == '__main__':
    main()