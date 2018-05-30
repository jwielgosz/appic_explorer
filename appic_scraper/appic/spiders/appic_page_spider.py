import scrapy
import collections
import re
from unidecode import unidecode

def clean_val(s):
    return re.sub(' +', ' ', unidecode(s).strip())

def clean_key(s):
    return re.sub(' +', ' ', unidecode(s).strip()).replace(':', '')


# --------------------------------------------------------------
# code to download raw HTML pages
#         filename = response.url.split("?")[-1] + '.html'
#         with open(filename, 'wb') as f:
#             f.write(response.body)

# Useful test pages

# https://membership.appic.org/directory/search?search=1&program_type_id=1&ID_MSI_USA_Metro=22            
            
# ALL internship sites
# https://membership.appic.org/directory/search?search=1&program_type_id=1&school_name=&pdir_name=&lead_name=&appic_num=&PROG_Describe=&ID_MSI_KWORDS=&program_country_id=&ID_MSI_USA_Metro=22&APP_DUE_START=&APP_DUE_END=&POS_startdate_START=&POS_startdate_END=&ACCR_APA=&ACCR_CPA=&POS_FT_funded=&POS_PT_funded=&POS_FT_low=&POS_FT_high=&POS_PT_low=&POS_PT_high=&SUM_INTOTAL_1112=&STAFF_LICENSED_FT=&STAFF_LICENSED_PT=&ID_SFIP_SupervisePDs=&APP_INTERVENTION=&APP_ASSESSMENT=

# --------------------------------------------------------------

class APPICEntrySpider(scrapy.Spider):
# Parse an individual directory entry

    name = "appic_entry"
    allowed_domains = ["membership.appic.org"]
    start_urls = [
        "https://membership.appic.org/directory/display/383",
#        "file://test_page.html" # for testing the code
    ]

    def combine_th_td(self, tbody, prefix):
        th = [clean_key(prefix + s) for s in tbody.xpath('th/text()').extract()]
        td = [clean_val(s) for s in tbody.xpath('td/text()').extract()]
        return zip(th, td)

    def flatten_table(self, tbody, prefix):
        
        def row_zip(row):
            row_name = clean_key(''.join(row.xpath('th/text()').extract()))
            full_col_names = [prefix + row_name + ' - ' + c for c in col_names]
            row_vals = row.xpath('td/text()').extract()
            row_vals = [clean_val(s) for s in row_vals]
            return zip(full_col_names, row_vals)
        
        col_names = tbody.xpath('tr/th[position()>1]/text()').extract()
        col_names = [clean_key(c) for c in col_names]
        
        table_vals = []
        row_sels = tbody.xpath('tr[position()>1]')
        for r in row_sels:
            table_vals.extend(row_zip(r))
        
        return table_vals



    def parse(self, response):
        site_dict = collections.OrderedDict()

        tbody = response.xpath('//*[@id="member-site-info-table"]/tr')
        prefix = 'Member Info - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[3]/fieldset/table/tr')
        prefix = 'Accreditation - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[4]/fieldset/table/tr')
        prefix = 'Faculty/Staff - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        #start
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[1]/tr')
        prefix = 'Position - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        #slots (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[2]')
        prefix = 'Position - '
        site_dict.update(self.flatten_table(tbody, prefix))

        #stipend (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[2]')
        prefix = 'Position - '
        site_dict.update(self.flatten_table(tbody, prefix))

        #fringe
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[4]/tr')
        prefix = 'Position - '
        site_dict.update(self.combine_th_td(tbody, prefix))
         
        #workday
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[5]/tr')
        prefix = 'Position - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        #affiliated_full
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[6]/tr')
        prefix = 'Position - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        #affiliated_part
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[5]/fieldset/div/table[7]/tr')
        prefix = 'Position - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[6]/fieldset/table/tr')
        prefix = 'Application Process - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[6]/fieldset/table/tr')
        prefix = 'Application Process - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[8]/fieldset/div/table[1]/tr')
        prefix = 'Program Types - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[8]/fieldset/div/table[2]/tr')
        prefix = 'Beliefs - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        para = response.xpath('//*[@id="main"]/div[4]/div/div[9]/fieldset/div/p/text()')
        site_dict['Program Description'] = clean_val(''.join(para.extract()))
        
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[1]/tr')        
        prefix = 'Population - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[1]/tr')        
        prefix = 'Modalities - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[2]/tr')        
        prefix = 'Modalities - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[11]/fieldset/div/table[3]/tr')        
        prefix = 'Experience - '
        site_dict.update(self.combine_th_td(tbody, prefix))

        # internship class (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[12]/fieldset/table')
        prefix = 'Intern Class - '
        site_dict.update(self.flatten_table(tbody, prefix))
        
        # employment settings (tabular data)
        tbody = response.xpath('//*[@id="main"]/div[4]/div/div[14]/fieldset/table')
        prefix = 'Intern Employment - '
        site_dict.update(self.flatten_table(tbody, prefix))

        for key in site_dict:
            site_dict[key] = re.sub(' +', ' ', site_dict[key])

        return(site_dict)
        
