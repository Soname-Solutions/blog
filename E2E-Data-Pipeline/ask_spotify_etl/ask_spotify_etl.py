import argparse
import os
import sys
import shutil
import time
import logging
import logging.config
import luigi

from ask_spotify_etl_config import Config
from db_connector.mariadb_connector import MariaDBConnector
from luigi_tasks import AskSpotifyPipeline

config = Config()
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

logging.config.fileConfig(fname=os.path.join(project_root, 'config', 'logging.conf'))
logger = logging.getLogger()


def get_all_files(files_zone: str) -> list[str]:
    """ get all file names from source zone.
        list of csv file names is returned
    """
    logger.debug(f"find all files in {files_zone}")
    dir_files = os.listdir(files_zone)
    files = [file for file in dir_files if file.endswith('.csv')]

    return files


def get_processed_files() -> list[str]:
    """ get all processed files from db.
        return list of unprocessed files only.
    """
    logger.debug("select processed files from etl_control table")
    processed_files_list = []
    sql = "select file_name from etl_control where status = 'finished'"

    connector = MariaDBConnector()
    with connector:
        processed_files = connector.execute([sql])

    for processed_file in processed_files:
        processed_files_list.append(processed_file[0])

    return processed_files_list


def copy_files() -> list[str]:
    """ copy file from source zone into app landing zone.
        only unprocessed files are copied if they are not in app landing zone.
        list of copied files is returned.
    """
    logger.debug("copy unprocessed files from source zone to app app landing zone")
    file_landing_zone = config.get('path', 'file_landing_zone')
    file_source_zone = config.get("path", "file_source_zone")
    # all unprocessed file
    unprocessed_files = list(set(get_all_files(file_source_zone)) - set(get_processed_files()))
    # unprocessed file which are not yet in landing zone
    unprocessed_files_to_copy = list(set(unprocessed_files) - set(get_all_files(file_landing_zone)))

    if unprocessed_files_to_copy:
        unprocessed_files_path = list(
            map(lambda unprocessed_file: os.path.join(file_source_zone, unprocessed_file),
                unprocessed_files
                )
            )
        for file in unprocessed_files_path:
            shutil.copy(file, file_landing_zone)

    logger.debug("%s files are copied from source zone", len(unprocessed_files_to_copy))
    return unprocessed_files


def housekeeping(landing_zone_files):
    """delete processed files from app landing_zone"""
    file_landing_zone = config.get('path', 'file_landing_zone')

    for file in landing_zone_files:
        os.remove(os.path.join(file_landing_zone, file))
        logger.info(f"{file} is removed by housekeeping")


def start_luigi(unprocessed_files: list[str], dev_mode: bool):
    """start luigi pipeline. get result status of the pipeline exection."""

    logger.info(f'luigi will start for {unprocessed_files}')

    use_local_scheduler = dev_mode
    if not use_local_scheduler:
        os.environ['LUIGI_CONFIG_PATH'] = config.get('path','luigi_scheduler_conf')

    luigi_pipeline_result = luigi.build(
        [AskSpotifyPipeline(unprocessed_files)],
        detailed_summary=True,
        local_scheduler=use_local_scheduler,
        logging_conf_file=config.get('path','logging_file')
        )

    logger.info('all luigi pipelines are done')

    if luigi_pipeline_result.status == luigi.LuigiStatusCode.SUCCESS:
        logger.info(f"clean up landing zone from: {unprocessed_files}")
        housekeeping(unprocessed_files)

def main(dev_mode: bool = False):
    """infinite app orchestration"""

    logger.info("start ask_spotify_etl app")

    while True:
        unprocessed_files = copy_files()
        if unprocessed_files:
            start_luigi(unprocessed_files=unprocessed_files, dev_mode=dev_mode)
        else:
            logger.info("no files to start luigi")
        time.sleep(int(config.get('general', 'sleep_time')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="ASK SPOTIFY ETL PIPELINE"
    )

    parser.add_argument("-d",
                        "--dev",
                        action="store_true",
                        help="run pipeline on dev using luigi local scheduler")
    args = parser.parse_args()

    main(dev_mode=args.dev)
