import os
import re
import sys
import textwrap

def write_file(stepname, stored):
    print(stepname)
    tokens = stepname.split("-")
    assert len(tokens) == 2
    component = tokens[0]

    if not os.path.exists(component):
        os.mkdir(component)

    text = "".join(stored)
    text = textwrap.dedent(text)

    o = open(os.path.join(component, stepname + ".yaml"), "w+")
    o.write(text)
    o.close()

if len(sys.argv) != 2:
    print("Usage: {} <path-to-migration-md>".format(sys.argv[0]))
    exit(1)

f = open(sys.argv[1], "r")

codeblock = re.compile("\s*```(yaml)?")
meta = re.compile('\s*\[//\]: # "([^"]+-step\d+)"')
empty = re.compile('\s*\n')
state = "init"
for line in f:
    if state == "init":
        if codeblock.match(line):
            state = "store"
            stored = []
    elif state == "store":
        if codeblock.match(line):
            state = "afterstore"
        else:
            stored.append(line)
    elif state == "afterstore":
        if empty.match(line):
            continue
        state = "init"
        match = meta.match(line)
        if not match:
            continue
        stepname = match.group(1)
        write_file(stepname, stored)
