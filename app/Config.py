import os, xml.etree.ElementTree as ET

class Config:
    def __init__(self, xml_path, profile):
        tree = ET.parse('config.xml')

        root = tree.getroot()

        for p in root.findall('profile'):
            if p.get('name') == profile:
                self.table = str(p.find('table').text)
                self.retro = int(p.find('retroactive').text)
                self.selected_fields = str(p.find('selected_fields').text)
                self.incremental_field = str(p.find('incremental_field').text)

                self.conn_src = str(p.find('source').find('connection').text)
                self.conn_dest = str(p.find('destination').find('connection').text)

                self.ssl_req_src = int(p.find('source').find('ssl').find('required').text)
                self.ssl_req_dest = int(p.find('destination').find('ssl').find('required').text)

                self.ssl_ca_src = str(p.find('source').find('ssl').find('ca').text)
                self.ssl_key_src = str(p.find('source').find('ssl').find('key').text)
                self.ssl_cert_src = str(p.find('source').find('ssl').find('cert').text)

                self.ssl_ca_dest = str(p.find('destination').find('ssl').find('ca').text)
                self.ssl_key_dest = str(p.find('destination').find('ssl').find('key').text)
                self.ssl_cert_dest = str(p.find('destination').find('ssl').find('cert').text)

                self.offset_hours = int(p.find('offset').find('hours').text)
                self.offset_minutes = int(p.find('offset').find('minutes').text)

    def get_ssl_src(self):
        if self.ssl_req_src == 1:
            return {'ssl': {'cert':self.ssl_cert_src,'key':self.ssl_key_src,'ca':self.ssl_ca_src}}
        return dict()

    def get_ssl_dest(self):
        if self.ssl_req_dest == 1:
            return {'ssl': {'cert':self.ssl_cert_dest,'key':self.ssl_key_dest,'ca':self.ssl_ca_dest}}
        return dict()

    