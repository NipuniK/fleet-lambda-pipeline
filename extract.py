import docx
doc = docx.Document('EC8203_Fleet_Ops_Report.docx')
with open('EC8203_Fleet_Ops_Report.md', 'w') as f:
    for para in doc.paragraphs:
        f.write(para.text + '\n\n')
