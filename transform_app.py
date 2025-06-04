import subprocess

# List of scripts to run in sequence
scripts = [
    # "1_get_symbol.py",
    # "2_choosee_file.py",
    #"3_financial.py",
    # "4_price_2.py",
    # "6_total_compare.py",

    # "7_1_FIX.py",
    #"7_financial_merge.py",
    # "8_price_merge.py"

    #"9_final_combine.py"
    "crawl_financial.py"
]

for script in scripts:
    print(f"Running {script}...")
    try:
        result = subprocess.run(["python", script], check=True, capture_output=True, text=True)
        print(f"Output from {script}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script}:\n{e.stderr}")
        break
