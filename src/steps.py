
import glob

from datetime import datetime
from pathlib import Path
from pyspark.sql import DataFrame


def landing_to_row(logger):

    # the file system is mounted in the dataLake folder so it can be hardcoded
    found_file = glob.glob("dataLake/landing/*.csv")

    if len(found_file) == 0:
        logger.info("No files found in landing zone, nothing to process")

    # for the sake of simplicity let's assume there is only one file uploaded daily

    target = "dataLake/raw/{0}_{1}.csv".format(
        found_file.split('/')[2].split(".csv")[0],
        str(datetime.now().date())
    )

    Path(found_file[0]).rename(target)

    logger.info("moved file {} into raw zone".format(target))

    return target
