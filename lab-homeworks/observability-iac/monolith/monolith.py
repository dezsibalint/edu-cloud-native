#!/usr/bin/env python3
"""
Monolithic OCR Processing Application
--------------------------------------
This program demonstrates a complete processing pipeline implemented
monolithically with the following steps:

1. File Input (FileGrab)
2. PDF to Image Conversion (PDF-to-Image)
3. Image Preprocessing (Image Processing)
4. OCR Text Extraction (OCR)
5. Page Text Aggregation and Storage (Text Aggregation)
"""

from pathlib import Path
import cv2
import pytesseract
import numpy as np
import fitz
import time

# ===== Configuration =====
INPUT_PATH = "samples/<TEST>.pdf"
OUTPUT_DIR = Path("output")


# =============================================================
# FileGrab – Load input file
# =============================================================

def load_input(input_path: str):
    """Load PDF or image file and convert to list of images."""
    ext = Path(input_path).suffix.lower()
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    if ext in [".png", ".jpg", ".jpeg"]:
        print("[INFO] Single image file detected")
        img = cv2.imread(input_path)
        if img is None:
            raise ValueError(f"Failed to load image: {input_path}")
        return [{
            'image': img,
            'page_number': 1,
            'total_pages': 1
        }], 1
        
    elif ext == ".pdf":
        print("[INFO] PDF file detected. Converting to images...")
        return convert_pdf_to_images(input_path)
        
    else:
        raise ValueError("Unsupported file type. Use PDF, PNG, JPG, or JPEG files.")


# =============================================================
# PDF-to-Image – Convert PDF pages to images
# =============================================================

def convert_pdf_to_images(pdf_path):
    """
    Convert each page of PDF to image.
    Returns: (list of images, total page count)
    """
    pages = []
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    
    print(f"[INFO] Processing {total_pages} pages from PDF")
    
    for page_num in range(total_pages):
        page = doc[page_num]
        pix = page.get_pixmap(dpi=300)
        
        # Convert to OpenCV format
        img_array = np.frombuffer(pix.samples, dtype=np.uint8)
        img = img_array.reshape(pix.height, pix.width, pix.n)
        img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        
        # Save intermediate result
        page_number = page_num + 1
        out_path = OUTPUT_DIR / f"page_{page_number}.png"
        cv2.imwrite(str(out_path), img)
        print(f"[INFO] Saved: {out_path}")
        
        pages.append({
            'image': img,
            'page_number': page_number,
            'total_pages': total_pages
        })
    
    doc.close()
    return pages, total_pages


# =============================================================
# Image Processing – Preprocessing for OCR
# =============================================================

def preprocess_image(img, page_number: int):
    """
    Basic preprocessing for OCR:
    - Convert to grayscale
    - Apply Otsu's binarization
    These steps improve OCR accuracy.
    """
    print(f"[INFO] Preprocessing image for page {page_number}...")

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Otsu's thresholding
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    
    return binary


# =============================================================
# OCR – Text Extraction per page
# =============================================================

def extract_text(img, page_number: int) -> str:
    """Run OCR on a single image and return extracted text."""
    print(f"[INFO] Running OCR on page {page_number}...")
    
    config = "--oem 3 --psm 6"
    text = pytesseract.image_to_string(img, lang="eng", config=config)
    
    return text.strip()


# =============================================================
# Text Aggregation – Combine pages and save
# =============================================================

def aggregate_text(page_results: list) -> str:
    """
    Aggregate text from all pages in correct order.
    page_results: list of dicts with 'page_number' and 'text' keys
    """
    print("[INFO] Aggregating text from all pages...")
    
    # Sort by page number to ensure correct order
    page_results.sort(key=lambda x: x['page_number'])
    
    full_text = []
    for result in page_results:
        page_num = result['page_number']
        text = result['text']
        full_text.append(f"=== PAGE {page_num} ===\n{text}\n")
    
    return "\n".join(full_text)


def save_text(text: str, output_file: Path):
    """Save text to file."""
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"[INFO] Text saved to: {output_file}")


# =============================================================
# Main Pipeline
# =============================================================

def main():
    start_time = time.time()
    
    print("=" * 50)
    print("OCR Processing Pipeline - Monolithic Version")
    print("=" * 50)

    # Load file (converts PDF to images if needed)
    pages_data, total_pages = load_input(INPUT_PATH)
    print(f"[INFO] Loaded {total_pages} page(s)")

    # Process each page: preprocessing + OCR
    page_results = []
    
    for page_data in pages_data:
        img = page_data['image']
        page_num = page_data['page_number']
        
        # Preprocess image
        processed = preprocess_image(img, page_num)
        
        # Extract text via OCR
        text = extract_text(processed, page_num)
        
        page_results.append({
            'page_number': page_num,
            'text': text
        })
        
        print(f"[INFO] Page {page_num}/{total_pages} completed")

    # Aggregate all pages and save
    final_text = aggregate_text(page_results)
    save_text(final_text, OUTPUT_DIR / "final_result.txt")

    end_time = time.time()
    elapsed_time = end_time - start_time

    print("=" * 50)
    print("[INFO] Processing completed successfully!")
    print(f"[INFO] Total runtime: {elapsed_time:.2f} seconds")
    print("=" * 50)


if __name__ == "__main__":
    main()