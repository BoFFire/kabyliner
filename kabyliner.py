import os
import requests
import xml.etree.ElementTree as ET
import csv
from pathlib import Path

# ======================= TMX Download & Processing ========================
def download_tmx(url, local_filename):
    """Downloads TMX file with size validation."""
    if os.path.exists(local_filename):
        local_size = os.path.getsize(local_filename)
        head_response = requests.head(url, allow_redirects=True)
        remote_size = int(head_response.headers.get("Content-Length", -1))
        if remote_size == local_size:
            print(f"File '{local_filename}' exists with matching size. Skipping download.")
            return

    print(f"Downloading TMX file from {url}...")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_filename, "wb") as f:
            f.write(response.content)
        print(f"Download saved as: {local_filename}")
    else:
        raise Exception(f"Download failed. Status code: {response.status_code}")

def extract_parallel_corpus(tmx_file, tsv_file="parallel_corpus.tsv", src_lang="en", tgt_lang="kab"):
    """Extracts parallel corpus from TMX with namespace handling."""
    tree = ET.parse(tmx_file)
    root = tree.getroot()
    namespace = {'tmx': root.tag.split('}')[0][1:]} if '}' in root.tag else {'tmx': ''}

    with open(tsv_file, "w", encoding="utf-8") as out_file:
        out_file.write(f"{src_lang}\t{tgt_lang}\n")
        
        for tu in root.findall(".//tmx:tu", namespace):
            texts = {src_lang: [], tgt_lang: []}
            for tuv in tu.findall("tmx:tuv", namespace):
                lang = tuv.get("{http://www.w3.org/XML/1998/namespace}lang")
                if lang in [src_lang, tgt_lang]:
                    seg = tuv.find("tmx:seg", namespace)
                    if seg is not None:
                        text = ''.join(seg.itertext()).strip()
                        if text: texts[lang].append(text)
            
            if texts[src_lang] and texts[tgt_lang]:
                out_file.write(f"{' '.join(texts[src_lang])}\t{' '.join(texts[tgt_lang])}\n")
    
    print(f"Raw corpus extracted to {tsv_file}")

# ======================= Corpus Cleaning ========================
def clean_corpus(input_path, output_path, verbose=False):
    """Cleans the TSV corpus by removing invalid entries."""
    if not Path(input_path).exists():
        raise FileNotFoundError(f"Input file {input_path} not found")
    
    kept = 0
    removed = 0
    
    with open(input_path, 'r', encoding="utf-8") as infile, \
         open(output_path, 'w', encoding="utf-8", newline='') as outfile:
        
        reader = csv.reader(infile, delimiter='\t')
        writer = csv.writer(outfile, delimiter='\t')
        
        try:
            header = next(reader)
            if header != ['en', 'kab']:
                raise ValueError("Invalid header format. Expected ['en', 'kab']")
            writer.writerow(header)
        except StopIteration:
            raise ValueError("Empty input file")
        
        for row in reader:
            if len(row) == 2 and row[0].strip() and row[1].strip():
                writer.writerow(row)
                kept += 1
            else:
                removed += 1
            
            if verbose and (kept + removed) % 1000 == 0:
                print(f"Processed {kept + removed} rows...")
    
    print(f"\nCleaning results:")
    print(f"  Total rows processed: {kept + removed}")
    print(f"  Valid pairs kept: {kept}")
    print(f"  Invalid pairs removed: {removed}")
    
    return kept, removed

# ======================= File Splitting ========================
def split_tsv_to_txt(tsv_file, en_file="en.txt", kab_file="kab.txt"):
    """Splits cleaned TSV into separate text files."""
    with open(tsv_file, 'r', encoding="utf-8") as infile, \
         open(en_file, 'w', encoding="utf-8") as en_out, \
         open(kab_file, 'w', encoding="utf-8") as kab_out:
        
        next(infile)  # Skip header
        for line in infile:
            en, kab = line.strip().split('\t')
            en_out.write(f"{en}\n")
            kab_out.write(f"{kab}\n")
    
    print(f"Split into {en_file} and {kab_file}")

# ======================= Main Execution ========================
if __name__ == "__main__":
    # Configuration
    TMX_URL = "https://gitlab.com/imsidag/taqbaylit/-/raw/master/tmx/kabyle-tm.tmx?ref_type=heads"
    TMX_FILE = "kabyle-tm.tmx"
    RAW_TSV = "parallel_corpus.raw.tsv"
    CLEAN_TSV = "parallel_corpus.clean.tsv"
    EN_FILE = "en.txt"
    KAB_FILE = "kab.txt"

    try:
        # Pipeline execution
        download_tmx(TMX_URL, TMX_FILE)
        extract_parallel_corpus(TMX_FILE, RAW_TSV)
        kept, removed = clean_corpus(RAW_TSV, CLEAN_TSV, verbose=True)
        split_tsv_to_txt(CLEAN_TSV, EN_FILE, KAB_FILE)
        
        # Final report
        def count_lines(f): return sum(1 for _ in open(f, 'r', encoding="utf-8"))
        print("\nFinal counts:")
        print(f"- Cleaned translation pairs: {kept}")
        print(f"- Removed invalid pairs: {removed}")
        print(f"- English sentences: {count_lines(EN_FILE)}")
        print(f"- Kabyle sentences: {count_lines(KAB_FILE)}")
    
    except Exception as e:
        print(f"\nError in processing pipeline: {str(e)}")
        exit(1)
