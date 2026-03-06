# compare base.txt (A) with dnd.txt (B)
# output A - B into scrub.txt

fromfile = "dnd.txt"   # B
tofile = "base.txt"    # A
newfile = "scrub.txt"

# read all numbers from dnd.txt into a set
with open(fromfile, "r") as f:
    dnd_numbers = set(line.strip()[:13] for line in f if line.strip())

# open base.txt and write numbers not found in dnd.txt
with open(tofile, "r") as f_in, open(newfile, "w") as f_out:
    for line in f_in:
        mobile = line.strip()[:13]
        if mobile not in dnd_numbers:
            f_out.write(mobile + "\n")
            