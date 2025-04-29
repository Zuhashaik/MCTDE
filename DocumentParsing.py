import re
import os
import pypdf
import pymupdf
import logging
import tempfile
import traceback
from typing import List
from io import BytesIO
from collections import defaultdict
from trialmind.TrialDesign.KBService.textract_utils.pretty_print import get_text_from_layout_json
    
def create_textract_job(
    item_path_s3: str, 
    textract_output_path_s3: str
):
    """
    Extract text from the PDF file.
    """

    textract_job_response = textract_client.start_document_analysis(
        DocumentLocation={
            "S3Object": {
                "Bucket": s3_bucket_name,
                "Name": item_path_s3,
            }
        },
        FeatureTypes=["TABLES", "LAYOUT"],
        OutputConfig={
            "S3Bucket": s3_bucket_name,
            "S3Prefix": textract_output_path_s3,
        },
        NotificationChannel={
            "RoleArn": textract_sns_role_arn,
            "SNSTopicArn": textract_sns_topic_arn,
        },
    )
    job_id = textract_job_response["JobId"]
    print("Job ID From Textract: ", job_id)
    return job_id

def get_pdf_embedded_text(
    self,
    file_path: str,
) -> dict:
    """
    Extract embedded text from a PDF file and check if it contains actual text content.
    
    Args:
        file_path: Path to the PDF file
        
    Returns:
        A dictionary with:
        - 'text_by_page': Dictionary mapping page numbers to text content
        - 'has_text_content': Boolean indicating if the PDF has meaningful text content
        - 'total_text_length': Total length of extracted text across all pages
    """
    # Extract the embedded text
    document = pymupdf.open(file_path)
    
    # Dictionary: {page_num: text_content}
    all_text_by_page = defaultdict(str)
    total_text_length = 0
    total_valid_chars = 0
    
    for i, page in enumerate(document):
        # Get the text from the page
        text = page.get_text()
        
        if (len(all_text_by_page[i]) > 0):
            all_text_by_page[i] += "\n" + text
        else: 
            all_text_by_page[i] += text
        
        page_text = all_text_by_page[i].strip()
        total_text_length += len(page_text)
        
        # Count printable ASCII chars and common Unicode text characters
        valid_chars = sum(1 for c in page_text if (c.isprintable() and not c.isspace()) or 
                            (ord(c) > 127 and ord(c) < 10000))  # Include common Unicode ranges
        total_valid_chars += valid_chars
    
    # Check if the PDF has meaningful text content - multiple criteria:
    # 1. Minimum average characters per page
    # 2. Ratio of valid chars to total chars must be above threshold
    # 3. At least some meaningful words must be present (checked via regex)
    
    avg_text_per_page = total_text_length / max(1, len(document))
    
    # Calculate ratio of valid to total characters (excluding whitespace)
    non_whitespace_count = sum(1 for c in ''.join(all_text_by_page.values()) if not c.isspace())
    valid_char_ratio = total_valid_chars / max(1, non_whitespace_count)
    
    # Check for English words (a very basic test for actual text content)
    joined_text = ' '.join(all_text_by_page.values())
    # Look for common English words or patterns
    english_word_pattern = re.compile(r'\b(the|and|to|of|in|for|with|as|on|at|by|an|or|this|that|these|those)\b', 
                                        re.IGNORECASE)
    has_english_words = len(english_word_pattern.findall(joined_text)) > 5  # At least 5 common words
    
    # Control character detection - high presence suggests binary content not text
    control_chars = sum(1 for c in joined_text if ord(c) < 32 and c not in '\n\r\t')
    control_char_ratio = control_chars / max(1, len(joined_text))
    
    # Final decision based on multiple factors
    has_text_content = (avg_text_per_page > 50 and  # Minimum characters per page
                        valid_char_ratio > 0.7 and   # Most chars should be valid text
                        has_english_words and        # Should contain some English words
                        control_char_ratio < 0.05)   # Low percentage of control chars
    
    logging.info(f"PDF text analysis: avg_chars={avg_text_per_page:.1f}, valid_ratio={valid_char_ratio:.2f}, "
                    f"control_ratio={control_char_ratio:.2f}, has_english={has_english_words}")
    
    return {
        'text_by_page': all_text_by_page,
        'has_text_content': has_text_content,
        'total_text_length': total_text_length,
        'valid_char_ratio': valid_char_ratio,
        'control_char_ratio': control_char_ratio,
        'has_english_words': has_english_words
    }
    
def extract_text_from_pdf(
    kb_item_id: str,
    file_content: bytes,
):
    """
    Create a KB item, using the file content. Determines whether the PDF contains 
    embedded text or is image-based, and processes accordingly.
    """
    
    # ~~~~ DOCUMENT PRE-PROCESSING & RECORD CREATION ~~~~
    
    # this file needs to be deleted at the end of the function, otherwise
    # it will be left in the system, causing a memory leak
    temporary_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
    
    # Write the binary content to the temporary file
    temporary_file.write(file_content)
    temporary_file.flush()
    temporary_file.close()  # Close it so it can be opened by other processes

    # Check if the PDF has embedded text content
    embedded_text_info = get_pdf_embedded_text(temporary_file.name)
    
    # Determine processing path based on text content
    textract_job_id = None
    if not embedded_text_info['has_text_content']:
        logging.info(f"PDF {kb_item_id} appears to be image-based with little or no text content.")
        
        textract_job_id = create_textract_job(
            get_raw_item_storage_path(kb_item_id),
            get_textract_output_storage_path(kb_item_id)
        )
    
    # ~~~~ THE MAIN DOCUMENT PROCESSING ~~~~
    # in the case that we use textract, we call the same function later in the consumer
    if textract_job_id is None:    
        handle_extracted_text(
            kb_item_id=kb_item_id,
            text_by_page=embedded_text_info['text_by_page']
        )
        
def process_message(job_id: str):
    """
    Process a message from the SQS queue.
    """
    print(f"Processing message with JobId: {job_id}")
    try:
        kb_item_id = get_kb_item_from_textract_job_id(job_id)
    except Exception as e:
        update_job_status_by_textract_job_id(job_id, STATE_FAILED)
        logging.error(f"Failed to fetch KnowledgeBase item: {e}")
        raise e

    items = list(
        _get_textract_output_items(
            os.path.join(get_textract_output_storage_path(kb_item_id), job_id)
        )
    )
    
    assert len(items) > 0, "No items found in the textract output"
    
    all_blocks = []
    for item in items:
        all_blocks.extend(item["Blocks"])

    blocks = {
        "Blocks": all_blocks,
        "DocumentMetadata": items[0]["DocumentMetadata"],
    }
    
    full_text, _ = get_text_from_layout_json(
        textract_json=blocks,
        generate_markdown=True,
        exclude_page_header=True,
        exclude_page_footer=True,
    )
      
    handle_extracted_text(
        kb_item_id=kb_item_id,
        text_by_page=full_text
    )
    