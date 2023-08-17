import os
import re
import sys
import requests
import PyPDF2
import pdfplumber
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

# -------------- HELPER FUNCTIONS --------------


def clean_description(description_field):
    """
    Clean the description field by retaining only the last non-empty line before "Description".
    """
    if description_field:
        description_field = description_field[0].strip().split("\n")
        description_field = [line for line in description_field if line.strip()]
        description_field = description_field[-1] if description_field else None
    else:
        description_field = None
    description_field=description_field.replace(" ","-").replace("_","-")
    return description_field


def clean_field(field, limit=None):
    """
    Clean a given field. Optionally, restrict it to a fixed length (limit).
    """
    if field:
        field = field[0].strip().replace(" ", "_")
        if limit:
            field = (
                field[:limit]
                if len(field) >= limit
                else field + "_" * (limit - len(field))
            )
    else:
        field = None
    return field


def clean_date(date_field):
    """
    Clean date fields to a consistent format.
    """
    if date_field:
        date_field = date_field[0].split(" at")[0].strip().replace("/", "_")
        if date_field.replace(" ", "") == "UploadDate":
            date_field = ""
    else:
        date_field = None
    return date_field


# -------------- CORE FUNCTIONALITY --------------


def download_jpeg(url, path, count, section_title):
    """
    Download an image from the provided URL and save it to the specified path.
    """
    response = requests.get(url)
    filename = os.path.join(path, f"{section_title}_{count}.jpeg")
    with open(filename, "wb") as file:
        file.write(response.content)
    return filename, count


def download_jpegs_from_pdf(pdf_file, directory):
    """
    Download JPEG images linked within the provided PDF.
    """
    jpeg_files = {}
    with pdfplumber.open(pdf_file) as pdf, ThreadPoolExecutor(
        max_workers=10
    ) as executor:
        futures = []
        for page_number, page in enumerate(pdf.pages):
            uploaded_by, taken_date, upload_date, section_title, job_number = parse_text_to_fields(
                page.extract_text()
            )

            for link in page.hyperlinks:
                jpeg_url = link["uri"]
                futures.append(
                    executor.submit(
                        download_jpeg,
                        jpeg_url,
                        directory,
                        page_number + 1,
                        section_title,
                    )
                )
        for future in concurrent.futures.as_completed(futures):
            jpeg_file, page_number = future.result()
            jpeg_files[page_number] = jpeg_file
    return jpeg_files


def parse_text_to_fields(text):
    """
    Parse text from a PDF page to extract required fields.
    """
    text = text.replace("  ", " ")
    uploaded_by_pattern = r"Uploaded By\n(.*?)\n"
    taken_date_pattern = r"Taken Date\n(.*?)\n"
    upload_date_pattern = r"Upload Date\n(.*?)\n"
    description_pattern = r"(.*?)Description"
    job_number_pattern = r"Job #:\s(\d+)"  # Added pattern to capture job number

    uploaded_by = re.findall(uploaded_by_pattern, text, re.DOTALL)
    taken_date = re.findall(taken_date_pattern, text, re.DOTALL)
    upload_date = re.findall(upload_date_pattern, text, re.DOTALL)
    description = re.findall(description_pattern, text, re.DOTALL)
    job_number = re.findall(job_number_pattern, text)  # Added line to find job number

    uploaded_by = clean_field(uploaded_by, 5)
    taken_date = clean_date(taken_date)
    upload_date = clean_date(upload_date)
    description = clean_description(description)
    
    # Extract the first matched job number, if available
    job_number = job_number[0] if job_number else None

    return uploaded_by, taken_date, upload_date, description, job_number  # Added job_number to return values



def move_jpeg_to_directory(old_file, description):
    """
    Move JPEG files to the appropriate directory based on its description.
    """
    directory_path = os.path.join(os.path.dirname(old_file), description)
    os.makedirs(directory_path, exist_ok=True)
    new_file = os.path.join(directory_path, os.path.basename(old_file))
    os.rename(old_file, new_file)


def rename_jpeg(old_file, page_number, description, uploaded_by, taken_date,job_number):
    """
    Rename and reorganize JPEGs based on their metadata.
    """
    if taken_date == "":
        new_name_base = f"{description}_{page_number}_{job_number}"
    else:
        new_name_base = f"{description}_{page_number}_{taken_date}_{job_number}"
    new_name_base = re.sub(r"\W+", "", new_name_base)
    new_name = f"{new_name_base}.jpeg"
    suffix = 0
    while os.path.exists(os.path.join(os.path.dirname(old_file), new_name)):
        suffix += 1
        new_name = f"{new_name_base}_{suffix}.jpeg"
    new_file = os.path.join(os.path.dirname(old_file), new_name)
    try:
        os.rename(old_file, new_file)
        move_jpeg_to_directory(new_file, description)
    except Exception as e:
        print(f"Failed to rename {old_file} due to {str(e)}")


def process_pdf(pdf_file, directory):
    """
    Process a given PDF, download and reorganize its linked JPEGs.
    """
    os.makedirs(directory, exist_ok=True)

    jpeg_files = download_jpegs_from_pdf(pdf_file, directory)
    with open(pdf_file, "rb") as file:
        read_pdf = PyPDF2.PdfReader(file)
        number_of_pages = len(read_pdf.pages)
        for page_number in range(1, number_of_pages + 1):
            page = read_pdf.pages[page_number - 1]
            text = page.extract_text()
            uploaded_by, taken_date, upload_date, description,job_number = parse_text_to_fields(
                text
            )
            if page_number in jpeg_files:
                rename_jpeg(
                    jpeg_files[page_number],
                    page_number,
                    description,
                    uploaded_by,
                    taken_date,
                    job_number
                )


# -------------- MAIN EXECUTION BLOCK --------------

if __name__ == "__main__":
    
    pdf_file = sys.argv[1]
    directory = sys.argv[2]
    print(repr(directory))
    directory=directory.replace("\\\\","\\").replace("\"","")
    print(repr(directory))
    # Prints 'I\\Project' both times
    if len(sys.argv) != 3:
        print("Invalid arguments. Usage: <script_name> <path_to_pdf> <save_directory>")
        sys.exit(1)

    process_pdf(pdf_file, directory)
