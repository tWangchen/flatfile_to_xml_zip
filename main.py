import logging
import shutil
import time
from pathlib import Path

import polars as pl
from lxml import etree

# Define base directory for downloads and outputs
BASE_DIR = Path("./downloads")
BASE_DIR.mkdir(exist_ok=True)

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s: %(levelname)s:%(name)s: %(message)s")
file_handler = logging.FileHandler(BASE_DIR / "flatfile_to_xml_zip.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Define the batch size (number of rows to process at a time)
BATCH_SIZE = 10000

# Ensure the input flat-file and directories are set correctly
INPUT_FLAT_FILE = BASE_DIR / "metadata-dump-prod-10-records.csv"
XML_DIRECTORY = BASE_DIR / "xml"
XML_ZIP_FILE = BASE_DIR / "xml_compressed"
XML_DIRECTORY.mkdir(exist_ok=True)


def xml_to_ecatid_uuid(input_xml) -> list:
    """Convert an XML string into a list of strings or None values using XPath expressions."""

    NAMESPACES = {
        "mdb": "http://standards.iso.org/iso/19115/-3/mdb/2.0",
        "mcc": "http://standards.iso.org/iso/19115/-3/mcc/1.0",
        "cit": "http://standards.iso.org/iso/19115/-3/cit/2.0",
        "gco": "http://standards.iso.org/iso/19115/-3/gco/1.0",
    }

    XPATH_LIST = [
        (
            "//mdb:MD_Metadata/mdb:alternativeMetadataReference/cit:CI_Citation/cit:identifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            "ecatid",
        ),
        (
            "/mdb:MD_Metadata/mdb:metadataIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString/text()",
            "uuid",
        ),
    ]

    xml_tree = etree.fromstring(input_xml)
    ecatid_uuid_list = []
    for xpath, field in XPATH_LIST:
        result = xml_tree.xpath(xpath, namespaces=NAMESPACES)
        if result:
            ecatid_uuid_list.append(
                ", ".join(result)
            )  # Join xpath with multiple values
        else:
            ecatid_uuid_list.append(None)  # Append None if XPath does not exist

    return ecatid_uuid_list


def export_individual_xml(input_individual_xml, export_individual_xml_file) -> None:
    """Export the given XML string to a file."""
    export_path = Path(export_individual_xml_file)
    export_path.write_text(input_individual_xml, encoding="utf-8")


def compress_directory(input_xml_directory, output_zip_file) -> None:
    """Compress all XML files in the given directory into a single zip file."""
    shutil.make_archive(str(output_zip_file), "zip", input_xml_directory)


def main() -> None:
    """Main function to process the file-file in batches, extract XML data, and compress the results."""
    try:
        start = time.perf_counter()
        logger.info(f"Start processing {INPUT_FLAT_FILE}...")

        df = pl.read_csv(INPUT_FLAT_FILE, batch_size=BATCH_SIZE, low_memory=False)
        filtered_batch = df.filter(pl.col("istemplate") == "n")  # filter out templates

        for metadata in filtered_batch.iter_rows(named=True):
            logger.info(f"Processing metadata_id: {metadata['id']}")
            try:
                xml = metadata["data"]
                ecatid_uuid_list = xml_to_ecatid_uuid(
                    input_xml=xml,
                )
                logger.info(
                    f"Extracted ecatid_uuid_list: {ecatid_uuid_list} for metadata_id: {metadata['id']}"
                )
                ecatid_uuid = "_".join(ecatid_uuid_list)

                logger.info(
                    f"Exporting individual XML: {ecatid_uuid} for metadata_id: {metadata['id']}."
                )
                export_individual_xml(
                    input_individual_xml=xml,
                    export_individual_xml_file=XML_DIRECTORY / f"{ecatid_uuid}.xml",
                )
                logger.info(f"Completed processing metadata_id: {metadata['id']}")
            except Exception as e:
                logger.exception(f"Error processing metadata_id {metadata['id']}: {e}")

        logger.info("Starting compression of XML files...")
        compress_directory(
            input_xml_directory=XML_DIRECTORY,
            output_zip_file=XML_ZIP_FILE,
        )

        logger.info(
            f"Completed {filtered_batch.shape[0]} rows in {time.perf_counter() - start:0.2f} seconds."
        )
    except Exception as e:
        logger.exception(f"Error processing {INPUT_FLAT_FILE}: {e}")


if __name__ == "__main__":
    main()
