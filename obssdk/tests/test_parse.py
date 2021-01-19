import unittest

from obs.xml_parse import CommonXmlResponseHandler


class VirtualResponse(object):

    def __init__(self, text):
        self._text = text

    @property
    def text(self):
        return self._text


class TestParser(unittest.TestCase):

    def test_parse(self):
        xml = list()
        xml.append(r'<?xml version="1.0"?>')
        xml.append(r'<root>')
        xml.append(r'<name>')
        xml.append(r'tomcat')
        xml.append(r'</name>')
        xml.append(r'<age>')
        xml.append(r'18')
        xml.append(r'</age>')
        xml.append(r'</root>')

        response = VirtualResponse(''.join(xml))
        handler = CommonXmlResponseHandler(['root'], ['name', 'age'])
        result = handler.handle_success(response)
        self.assertEqual('root', result.root_ele_name)
        self.assertEqual('tomcat', result.ele_names['name'])
        self.assertEqual('18', result.ele_names['age'])


if __name__ == "__main__":
    unittest.main()
