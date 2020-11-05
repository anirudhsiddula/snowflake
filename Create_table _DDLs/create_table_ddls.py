import os
import re
import glob
file_names = glob.glob("/home/batman/files/Webster/local_csv/regex_test/*.csv")
commands = []
for filename in file_names:
    commands.append("csvsql -i oracle -d '|' -q \\\" -p \\\\ "+filename+ ' >> test.sql')
#print(commands)
for command in commands:
    os.system(command)
    print(command)

with open("/home/batman/files/Webster/local_csv/regex_test/test.sql") as insqlfile:
    lines = insqlfile.readlines()

pattern = re.compile(r"\.\d{8}")

with open("/home/batman/files/Webster/local_csv/regex_test/final_output.sql","w") as outsqlfile:
    for line in lines:
        match = re.search(pattern, line)
        if match:
            newline = re.sub(pattern,"",line)
            outsqlfile.write(newline)
        else:
            outsqlfile.write(line)

