from lxml import etree
import logging

class Validator:

    def __init__(self, xsd_path: str, logger):
        self.logger = logger
        xmlschema_doc = etree.parse(xsd_path)
        self.xmlschema = etree.XMLSchema(xmlschema_doc)

    def validate(self, xml_path: str) -> bool:

        #result = self.xmlschema.validate(xml_doc)
        try:
            doc = etree.parse(xml_path)
            self.xmlschema.assertValid(doc)
            #print('XML valid, schema validation ok.')
            return True
        except etree.DocumentInvalid as err:
            #print('Schema validation error, see error_schema.log')
            #with open('error_schema.log', 'w') as error_log_file:
            #    error_log_file.write(str(err.error_log))
            self.logger.error(err, exc_info=True)
            return False
        except Exception as e:
            self.logger.error(e, exc_info=True)
            return False
    pass

