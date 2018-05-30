import scrapy

import collections
import re
import math

from unidecode import unidecode

def clean_val(s):
    return re.sub('\s+', ' ', unidecode(s).strip())

def clean_key(s):
    return re.sub('\s+', ' ', unidecode(s).strip()).replace(':', '')

def flatten_table(tbody, prefix):
    def row_zip(row):
        row_name = clean_key(''.join(row.xpath('string(th)').extract()))
        full_col_names = [prefix + row_name + ' - ' + c for c in col_names]
        row_sels = row.xpath('td')
        row_vals = [clean_val(''.join(s.xpath('string()').extract())) for s in row_sels]
        return zip(full_col_names, row_vals)
    col_names = tbody.xpath('tr/th[position()>1]/text()').extract()
    col_names = [clean_key(c) for c in col_names]
    table_vals = []
    row_sels = tbody.xpath('tr[position()>1]')
    for r in row_sels:
        table_vals.extend(row_zip(r))
    return table_vals


def combine_th_td(tbody, prefix):
    th = [clean_key(prefix + s) for s in tbody.xpath('string(th)').extract()]
    td = [clean_val(s) for s in tbody.xpath('string(td)').extract()]
    return zip(th, td)




class APPICSpider(scrapy.Spider):
# Top level crawler for APPIC Online Directory
# Obtains a list of sites
# Invokes appic_page_spider.py to parse individual entries
    name = "appic"
    allowed_domains = ["membership.appic.org"]
    start_urls = [
    
         ## ALL 768 SITES ##
         "https://membership.appic.org/directory/search?search=1&program_type_id=1" 
         
         
#        "https://membership.appic.org/directory/search?search=1&program_type_id=1&ID_MSI_USA_Metro=22", # single page
#        "https://membership.appic.org/directory/search?search=1&program_type_id=1&ID_MSI_USA_Metro=2", #LA

        #ALL p1 = "https://membership.appic.org/directory/search?search=1&program_type_id=1&p=1"
    ]

# next page //*[@id="main"]/div[2]/ul/li[3]/a


    def parse(self, response):
    
        nr_str = response.xpath('//*[@id="main"]/div[1]/strong/text()').extract()
        num_results = int(''.join(nr_str))

        entry_links = response.xpath("//*[@id=\"dir-results-table\"]/tbody/tr/td[2]/a/@href").extract()
        num_per_page = len(entry_links)

        num_pages = int(math.ceil(float(num_results) / num_per_page))

        print 'TOTAL ENTRIES: %d' % num_results
        print 'ENTRIES PER PAGE: %d' % num_per_page
        print 'NUMBER OF PAGES: %d' % num_pages

        
        for i in range(1, num_pages + 1):
            next_page = response.url + '&p=' + str(i)
            yield scrapy.Request(next_page, callback=self.parse_entry_links)
    

    def parse_entry_links(self, response):
        entry_links = response.xpath("//*[@id=\"dir-results-table\"]/tbody/tr/td[2]/a/@href").extract()
        for link in entry_links:
            complete_url = response.urljoin(link)
            yield scrapy.Request(complete_url, callback=self.parse_dir_entry)
    

    def parse_dir_entry(self, response):
        site_dict = collections.OrderedDict()

        site_dict['URL'] = response.url

        tbody = response.xpath('//*[@id="member-site-info-table"]/tr')
        prefix = 'Member Info - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[3]/fieldset/table/tr')
        prefix = 'Accreditation - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[4]/fieldset/table/tr')
        prefix = 'Faculty/Staff - '
        site_dict.update(combine_th_td(tbody, prefix))

        #start
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[1]/tr')
        prefix = 'Position - '
        site_dict.update(combine_th_td(tbody, prefix))

        #slots (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[2]')
        prefix = 'Position - '
        site_dict.update(flatten_table(tbody, prefix))

        #stipend (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[3]')
        prefix = 'Position - '
        site_dict.update(flatten_table(tbody, prefix))
                               #//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[3]/tbody/tr[1]/th[2]

        #fringe
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[4]/tr')
        prefix = 'Position - '
        site_dict.update(combine_th_td(tbody, prefix))
         
        #workday
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[5]/tr')
        prefix = 'Position - '
        site_dict.update(combine_th_td(tbody, prefix))

        #affiliated_full
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[6]/tr')
        prefix = 'Position - '
        site_dict.update(combine_th_td(tbody, prefix))

        #affiliated_part
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[7]/tr')
        prefix = 'Position - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[6]/fieldset/table/tr')
        prefix = 'Application Process - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[7]/fieldset/table/tr')
        prefix = 'Application Process - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[8]/fieldset/div/table[1]/tr')
        prefix = 'Program Types - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[8]/fieldset/div/table[2]/tr')
        prefix = 'Beliefs - '
        site_dict.update(combine_th_td(tbody, prefix))

        para = response.xpath('//*[@id="main"]/div[4]/div/div[9]/fieldset/div/p/text()')
        site_dict['Program Description'] = clean_val(''.join(para.extract()))
        
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[1]/tr')        
        prefix = 'Population - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[2]/tr')        
        prefix = 'Modalities - '
        site_dict.update(combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[3]/tr')        
        prefix = 'Experience - '
        site_dict.update(combine_th_td(tbody, prefix))

        # internship class (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[12]/fieldset/table')
        prefix = 'Intern Class - '
        site_dict.update(flatten_table(tbody, prefix))
        
        # employment settings (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[14]/fieldset/table')
        prefix = 'Intern Employment - '
        site_dict.update(flatten_table(tbody, prefix))

        for key in site_dict:
            site_dict[key] = re.sub(' +', ' ', site_dict[key])

        return(site_dict)
