from wordcloud import WordCloud
import xml.etree.ElementTree as elementtree


allText = ""

dom = elementtree.parse('senate-lobbying-2013_1_1_1.xml')
#retrieve the first xml tag (<tag>data</tag>) that the parser finds with name tagName:
filinglist = dom.getroot()
for filing in filinglist:
    issues = list(filing.getiterator('Issues'))
    if len(issues) > 0:
        issuelist = issues[0].getiterator('Issue')
        for i in issuelist:
            allText = allText + ' ' +  i.attrib.get('SpecificIssue')

wordcloud = WordCloud().generate(allText)
img = wordcloud.to_image()
img.save("senate-wordcloud.png")

    

