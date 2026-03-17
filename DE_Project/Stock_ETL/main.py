import logging
import os
from pipeline.extract   import extract_stock_data
from pipeline.transform import clean_and_transform
from pipeline.load      import load_to_duckdb

# Logging 
# Create Output Folder
os.makedirs("outputs", exist_ok=True)

# Log Config Time, Info, Message
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("outputs/pipeline.log", mode="a"),
    ]
)
logger = logging.getLogger(__name__)


# Pipeline Message
def run():
    logger.info("Pipeline start")

    logger.info("[1/3] Extract")
    raw = extract_stock_data()

    logger.info("[2/3] Transform")
    clean = clean_and_transform(raw)

    logger.info("[3/3] Load")
    load_to_duckdb(clean)

    logger.info("Pipeline complete")
    logger.info("Output: outputs/stocks.duckdb")
    logger.info("Log:    outputs/pipeline.log")


if __name__ == "__main__":
    run()
