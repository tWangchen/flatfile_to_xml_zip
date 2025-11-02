# Python tool. 
**Tool to extract individual XML records from flat-file, rename with relevant id prefix, and compress them into a ZIP archive.** 


## Features
- Reads a flatfile(CSV) as input. 
- Filters out templates. 
- Extracts individual XML records. 
- Extracts specific data (ECATID and UUID) from the XML records using XPath expressions. 
- Exports individual XML files with it's IDs(ECATID_UUID) prefixed. 
- Compresses all XML files into a single zip archive. 


## Usage

### Configure parameters in the code:  
`INPUT_FLAT_FILE`: Path to the input CSV file   
`XML_DIRECTORY`: Directory where XML files will be exported  
`XML_ZIP_FILE`: Path to the output zip archive.   

### Run script natively. 
Run the script with UV: `uv run main.py` or Python: `python main.py`

### Docker 
Use the following command:     
docker build: `docker build --pull --platform linux/amd64 -t flatfile-xml-zip .`  
docker run: `docker run --platform linux/amd64 -v "Your/Local/MountPath":/app/downloads flatfile-xml-zip`    
example: `docker run --platform linux/amd64 -v "/Users/twangchen/personalgit/github/flatfile_to_xml_zip/downloads":/app/downloads flatfile-xml-zip`  

## Dependencies
- Python 3.x 
- polars 
- lxml 


## Notes
Only maintained as required.  

