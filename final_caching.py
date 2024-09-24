from dotenv import load_dotenv
from langchain_community.document_loaders import PyPDFLoader
import boto3
from langchain_community.document_loaders import AmazonTextractPDFLoader
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_aws import ChatBedrock, BedrockLLM
from botocore.config import Config
import os
import json
from metaprompt import json_template
load_dotenv()
retry_config = Config(
    region_name=os.environ.get("region_name"),
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    }
)

session = boto3.Session()
client = session.client(service_name='bedrock-runtime', 
                        aws_access_key_id = os.environ.get("aws_access_key_id"),
                        aws_secret_access_key = os.environ.get("aws_secret_access_key"),
                        config=retry_config)

model = ChatBedrock(client=client, 
                    model_id=os.environ.get("BEDROCK_MODEL"),                   
                    )


# Load the document
def load_data(file_path):
    loader = AmazonTextractPDFLoader(file_path)
    docs = loader.load()
    return docs

document_data = load_data("/home/chandan/Projects/summarization/data/005.pdf")


def get_llm_response(model, pdf_data, json_template, processed_data=""):
    prompt = PromptTemplate(
        template="""
        You are an AI assistant specializing in extracting structured data from complex billing documents. Your task is to analyze the provided JSON data, which contains text, tables, and forms extracted from a billing PDF document. It is CRITICAL that you extract the data fields mentioned in the template JSON provided below for EACH billing item found in the parsed PDF. Each data point must be output as a key-value pair, one per line.
        IMPORTANT INSTRUCTIONS:
        1. Analyze EVERY SINGLE ELEMENT of the provided data: all text, every cell in every table, and all form fields.
        2. Identify ALL individual Billing information in the document. Each bill typically has its own section or table.
        3. For EACH bill item, create a separate instance of the template JSON and output each field as a key-value pair, one per line.
        4. Format: FIELD_NAME=VALUE
        5. If you can't find any data for the key in the JSON, output an empty string for that key. But make sure the KEY is present.
        6. Start each bill data with `MEDICAL_SERVICE_PROVIDER=MedicalServiceProviderName`. This will act as a marker to separate data between different billing items.
        7. Ensure that every response is a valid key-value pair list without any additional characters such as backslashes (`\`) or quotes.
        8. Ensure all dates are in proper format and all numerical values are appropriately typed (integer or float).
        9. Do not include any explanatory text, prefixes, or suffixes. Only provide the key-value pairs.
        10. Process ALL pages and ALL billing items in the document.
        11. You are strictly prohibited from hallucinating any data.
        12. AMOUNT_CHARGED cna never be negative, if you find one convert it to POSITIVE.
        13. Start processing from where the data below ends (processed so far):
        {processed_data}
        ACCURACY CHECK:
        - After extraction, review your output to ensure EVERY SINGLE data point for EACH billing item is captured in the key-value pairs.
        - Ensure no data point is duplicated or misplaced.
        - Ensure all key-value pairs are correct, consistent, and complete for each bill.
        Here is the template JSON data containing keys to populate for each billing item:
        {json_template}
        Here is the parsed data from a billing PDF document to analyze:
        {pdf_data}
        Respond with a list of key-value pairs, one per line, ensuring EVERY SINGLE critical data point from the template is captured for EACH billing data found in the document. Start each billing item's data with `MEDICAL_SERVICE_PROVIDER=MedicalServiceProviderName`.
        """,
        input_variables=["json_template", "pdf_data", "processed_data"],
    )

    # Chain execution
    chain = prompt | model | StrOutputParser()
    response = chain.invoke({
        "json_template": json_template,
        "pdf_data": pdf_data,
        "processed_data": processed_data,
    })
    
    return response


def extract_all_data(model, pdf_data, json_template):
    extracted_data = ""
    iteration_count = 0
    max_iterations = 3  # Set a limit for max attempts to avoid infinite loops
    
    while iteration_count < max_iterations:
        iteration_count += 1
        print(f"LLM CALL COUNT: {iteration_count}")
        
        # Call LLM with the full document but with processed data to help it continue
        response = get_llm_response(
            model=model, 
            pdf_data=pdf_data, 
            json_template=json_template, 
            processed_data=extracted_data  # Pass previously extracted data
        )

        # If no new data is found, stop
        if not response or "MEDICAL_SERVICE_PROVIDER" not in response:
            print("No new data found. Stopping extraction.")
            break

        # Append new response to extracted data
        extracted_data += response

    return extracted_data

# Usage example
result = extract_all_data(model, pdf_data=document_data, json_template=json_template)
print(result)

# Combine and parse the results
# def combine_results(all_results):
#         # Split the string into individual records
#     records = all_results.split('\n\n')

#     # Convert each record into a dictionary
#     json_list = []
#     for record in records:
#         entry = {}
#         lines = record.split('\n')
#         for line in lines:
#             if '=' in line:
#                 key, value = line.split('=', 1)
#                 entry[key.strip()] = value.strip()
#         json_list.append(entry)

#     # Convert the list of dictionaries to JSON format
#     json_output = json.dumps(json_list, indent=4)
#     return json_output

# all_results = combine_results(result)


# combined_data = combine_results(all_results)

# # Convert to JSON
# json_output = json.dumps(combined_data, indent=4)

# # Output the final JSON data
# print(json_output)

# Split the string into individual records
records = result.split('\n\n')

# Convert each record into a dictionary
json_list = []
for record in records:
    entry = {}
    lines = record.split('\n')
    for line in lines:
        if '=' in line:
            key, value = line.split('=', 1)
            entry[key.strip()] = value.strip()
    json_list.append(entry)

# Convert the list of dictionaries to JSON format
json_output = json.dumps(json_list, indent=4)

# Output the JSON
print(json_output)
entries = json.loads(json_output)
type(entries)
len(entries)
# Remove duplicates and incomplete entries
unique_entries = []
seen_entries = set()

# for entry in entries:
#     entry_tuple = tuple(entry.items())
#     if entry_tuple not in seen_entries:
#         seen_entries.add(entry_tuple)
#         unique_entries.append(entry)

# len(unique_entries)
# # Print the unique entries
# for entry in unique_entries:
#     print(entry)
required_keys = {
    "MEDICAL_SERVICE_PROVIDER",
    "DATE_OF_SERVICE",
    "PAGE_NO",
    "DESCRIPTION",
    "CPT_CODE",
    "ICD_CODE",
    "AMOUNT_CHARGED",
    "INSURANCE_PAID",
    "INSURANCE_ADJUSTMENT",
    "PLAINTIFF_PAID"
}
for entry in entries:
    # Check if all required keys are present
    if all(key in entry for key in required_keys):
        entry_tuple = tuple(entry.items())
        if entry_tuple not in seen_entries:
            seen_entries.add(entry_tuple)
            unique_entries.append(entry)

len(unique_entries)

import pandas as pd
df = pd.DataFrame(unique_entries)
df.head(20)

df.to_excel("billing_data_5.xlsx", index=False)