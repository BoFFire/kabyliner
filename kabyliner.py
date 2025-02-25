import os
import requests
import xml.etree.ElementTree as ET

def download_tmx(url, local_filename):
    """
    Downloads the TMX file from the given URL and saves it locally.
    
    If the file already exists locally, it compares the local file size with
    the remote file size. If sizes match, the download is skipped.
    """
    # Check if the file exists locally.
    if os.path.exists(local_filename):
        local_size = os.path.getsize(local_filename)
        # Get remote file size using a HEAD request.
        head_response = requests.head(url, allow_redirects=True)
        remote_size = int(head_response.headers.get("Content-Length", -1))
        if remote_size == local_size:
            print(f"File '{local_filename}' already exists with matching size ({local_size} bytes). Skipping download.")
            return
        else:
            print(f"Local file '{local_filename}' exists but sizes differ (local: {local_size} bytes, remote: {remote_size} bytes). Redownloading.")

    print(f"Downloading TMX file from {url} ...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_filename, "wb") as f:
            f.write(response.content)
        print(f"Downloaded TMX file saved as: {local_filename}")
    else:
        raise Exception(f"Failed to download TMX file. Status code: {response.status_code}")

def extract_parallel_corpus(tmx_file, tsv_file="parallel_corpus.tsv", src_lang="en", tgt_lang="kab"):
    """
    Extracts a parallel corpus from a TMX file and saves it as a TSV file.
    
    Handles multiple segments (e.g., plural or multi-paragraph entries) for each
    language by joining them with a space.
    """
    tree = ET.parse(tmx_file)
    root = tree.getroot()
    
    with open(tsv_file, "w", encoding="utf-8") as out_file:
        # Write header
        out_file.write(f"{src_lang}\t{tgt_lang}\n")
        
        for tu in root.findall(".//tu"):
            # Collect segments in lists for each language.
            texts = {src_lang: [], tgt_lang: []}
            
            for tuv in tu.findall("tuv"):
                lang = tuv.get("{http://www.w3.org/XML/1998/namespace}lang")
                if lang in [src_lang, tgt_lang]:
                    seg = tuv.find("seg")
                    if seg is not None and seg.text:
                        texts[lang].append(''.join(seg.itertext()).strip())
            
            # Write pairs only if both languages have at least one segment.
            if texts[src_lang] and texts[tgt_lang]:
                src_text = " ".join(texts[src_lang])
                tgt_text = " ".join(texts[tgt_lang])
                out_file.write(f"{src_text}\t{tgt_text}\n")
    
    print(f"Parallel corpus extracted to {tsv_file}")

def split_tsv_to_txt(tsv_file, en_file="en.txt", kab_file="kab.txt"):
    """
    Splits a TSV file (with a header) into two separate text files:
    one for English and one for Kabyle.
    """
    with open(tsv_file, "r", encoding="utf-8") as infile, \
         open(en_file, "w", encoding="utf-8") as en_out, \
         open(kab_file, "w", encoding="utf-8") as kab_out:
        
        next(infile)  # Skip the header line
        
        for line in infile:
            parts = line.strip().split("\t")
            if len(parts) == 2:
                en_sentence, kab_sentence = parts
                en_out.write(en_sentence + "\n")
                kab_out.write(kab_sentence + "\n")
    
    print(f"Extracted sentences to {en_file} and {kab_file}")

def count_lines(file_path):
    """Counts the number of lines in a file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f)

if __name__ == "__main__":
    # URL for the TMX file.
    tmx_url = "https://gitlab.com/imsidag/taqbaylit/-/raw/master/tmx/kabyle-tm.tmx?ref_type=heads"
    local_tmx = "kabyle-tm.tmx"
    
    # Download the TMX file (if necessary).
    download_tmx(tmx_url, local_tmx)
    
    # File names for the outputs.
    tsv_filename = "parallel_corpus.tsv"
    en_filename = "en.txt"
    kab_filename = "kab.txt"
    
    # Step 1: Extract the parallel corpus from the TMX file.
    extract_parallel_corpus(local_tmx, tsv_filename, src_lang="en", tgt_lang="kab")
    
    # Step 2: Split the TSV file into en.txt and kab.txt.
    split_tsv_to_txt(tsv_filename, en_filename, kab_filename)
    
    # Step 3: Count the number of lines in each file.
    # For the TSV file, subtract 1 to ignore the header.
    tsv_lines = count_lines(tsv_filename) - 1  
    en_lines = count_lines(en_filename)
    kab_lines = count_lines(kab_filename)
    
    print(f"Number of translation pairs (TSV, excluding header): {tsv_lines}")
    print(f"Number of English sentences (en.txt): {en_lines}")
    print(f"Number of Kabyle sentences (kab.txt): {kab_lines}")
