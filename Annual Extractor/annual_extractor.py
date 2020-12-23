import copy, logging, math, inspect, os, sys, re, operator
import arrow
import pyexcel as xl
import pyodbc
import smtplib

from os import path
from datetime import datetime, date
from pathlib import Path
from pyexcel import Book
from decimal import Decimal, DivisionByZero, InvalidOperation, getcontext
from operator import itemgetter
from shutil import copyfile

log_format = logging.Formatter('%(asctime)s - %(message)s')

annual_log_dir = os.path.join(os.getcwd(),"annual_logs")
if not os.path.exists(annual_log_dir): # If log director does not exist create it
    os.makedirs(annual_log_dir)

# Setup the log files to report user issues
user_logger = logging.getLogger('annual_user_logger')
user_logger.setLevel(logging.DEBUG)
user_log_file = '{}/annual_user.log'.format(annual_log_dir)
aulf = logging.FileHandler(user_log_file, 'w+')
aulf.setFormatter(log_format)
user_logger.addHandler(aulf)

# Setup the log files to report admin issues
admin_logger = logging.getLogger('annual_admin_logger')
admin_logger.setLevel(logging.DEBUG)
admin_log_file = '{}/annual_admin.log'.format(annual_log_dir)
aalf = logging.FileHandler(admin_log_file, 'w+')
aalf.setFormatter(log_format)
admin_logger.addHandler(aalf)

annual_data = os.path.join(os.getcwd(), "annual_data")
if not os.path.exists(annual_data): # If data director does not exist create it
    os.makedirs(annual_data)
annual_file = '{}/annual.xlsx'.format("annual_data")
date_annual_file = '{}/{}_annual.xlsx'.format("annual_data",date.today().strftime("%Y%m%d"))

if __name__ == '__main__':
    admin_logger.info('*** Started Annual file extraction ***')

    ########################## Declare the variables needed for extrating the data #########################
    admin_logger.info('DEC ~ Declaring global variables')
    base_dir = None
    bloomberg_price_data = None
    tp_years = None
    upside_years = None
    net_sales_years = None
    current_fy = None
    years_map = []
    tpdate = date.today()
    active_companies_list = []
    common_headers = ['No', 'Code', 'Company', 'Sector']
    annual_output = {}

    ########################## Extract all settings from each tab and get required settings  #########################
    admin_logger.info('CONF ~ Extracting configurations')
    config_file = os.path.join(os.getcwd(),'annual_config.xlsx')
    book = xl.get_book(file_name=config_file)
    for config_sheet in book.sheet_names():
        configsheet = book.sheet_by_name(config_sheet)
        if config_sheet == 'Generic':
            for cg_index, cg_row in enumerate(configsheet.rows()):
                if cg_row[0] == "TP_Years":
                    start_year, end_year = cg_row[1].strip().split('-')
                    tp_years_range = ['FY{}'.format(num) for num in range(int(start_year), int(end_year) + 1)]
                elif cg_row[0] == "TP_Years_Skip":
                    start_year, end_year = cg_row[1].strip().split('-')
                    tp_years_skip = ['FY{}'.format(num) for num in range(int(start_year), int(end_year) + 1)]
                elif cg_row[0] == "Upside_Years_Range":
                    start_year, end_year = cg_row[1].strip().split('-')
                    upside_years_range = ['FY{}'.format(num) for num in range(int(start_year), int(end_year) + 1)]
                elif cg_row[0] == "NetSales_Years_Range":
                    start_year, end_year = cg_row[1].strip().split('-')
                    pi_net_sales_years = ['FY{}'.format(num) for num in range(int(start_year), int(end_year) + 1)]
                elif cg_row[0] == "Current_FY":
                    current_fy = cg_row[1]
        elif config_sheet == 'Companies':
            companies_sheet = configsheet
        elif config_sheet == 'Filepaths':
            for fp_index, fp_row in enumerate(configsheet.rows()):
                if fp_row[0] == 'BaseDir':
                    base_dir = Path(fp_row[2])
                elif fp_row[0] == 'BloombergPriceData':
                    price_data_path = Path(fp_row[2])
                    bloomberg_price_data = base_dir.joinpath(price_data_path)
        elif config_sheet == 'Years':
            years_map = []
            years_map = [i for i in configsheet.rows()]

    ######################### From the companies sheet in annual_config.xlsx, create a list of only active companies for which data is extracted #########################
    admin_logger.info('ACL ~ Extracting list of Active companies')
    for acl_index, acl_row in enumerate(companies_sheet.rows()):
        if acl_row[4] != 1:
            continue
        kpi_company_name = '{} IN Equity'.format(acl_row[0])
        kpi_company_data = {'code': acl_row[0], 'bloomberg_code': kpi_company_name, 'name': acl_row[1], 'sector': acl_row[2], 'isbank': acl_row[3], 'active': acl_row[4]}
        active_companies_list.append(kpi_company_data)

    ########################## Extract headers from each model file and create the concatenated master list of headers #########################
    admin_logger.info('CHL ~ Extracting cumulative headers list')
    headers_list = []
    for chl_row in active_companies_list:
        header_file_open_error = 'No'
        header_file_xlsm = base_dir.joinpath('{}/models/{}.{}'.format(chl_row['sector'], chl_row['code'], 'xlsm'))
        header_file_xlsx = base_dir.joinpath('{}/models/{}.{}'.format(chl_row['sector'], chl_row['code'], 'xlsx'))
        if not path.exists(header_file_xlsm):
            if not path.exists(header_file_xlsx):
                user_logger.info('MODEL ISSUE ~ Skipping company {} within sector {} as model file {} or {} does not exists.'.format(chl_row['code'], chl_row['sector'], header_file_xlsx, header_file_xlsm))
                continue
            else:
                try:
                    headers_sheet = xl.get_sheet(file_name=str(header_file_xlsx), sheet_name='Annual')
                except:
                    user_logger.info('MODEL ISSUE ~ Skipping company {} within sector {} as could not open file {}.'.format(chl_row['name'], chl_row['sector']), str(header_file_xlsx))
                    header_file_open_error = 'Yes'
        else:
                try:
                    headers_sheet = xl.get_sheet(file_name=str(header_file_xlsm), sheet_name='Annual')
                except:
                    user_logger.info('MODEL ISSUE ~ Skipping company {} within sector {} as could not open file {}.'.format(chl_row['name'], chl_row['sector']), str(header_file_xlsm))
                    heaader_file_open_error = 'Yes'
        if header_file_open_error == 'Yes':
            continue
        got_headers = 'No'
        for chl_index, chl_data in enumerate(headers_sheet.rows()):
            if chl_data and chl_data[3] == 'INR mn' and got_headers == 'No':
                got_headers = 'Yes'
                for chl_col in range(len(chl_data)):
                    if chl_col < 4 or headers_sheet[chl_index, chl_col] == '':
                        continue
                    chl_year = headers_sheet[chl_index, chl_col]
                    for year_variant in years_map:
                        if chl_year in year_variant:
                            chl_year_mapped = str(year_variant[0])
                            break
                        else:
                            chl_year_mapped =  str('UMY_') + str(chl_year)
                    if not chl_year_mapped in headers_list:
                        headers_list.append(chl_year_mapped)
                break
    headers_list.append('zotal')
    headers_list.sort()

    ######################### Extract CMP, MKTCAP & Price Performance for all active companies #########################
    admin_logger.info('CMP + MKTCAP + PRICE PERFORMANCE ~ Started Extraction Of CMP, MKTCAP & Price Performance %')
    pd_file_open_error = 'No'
    try:
        price_data_sheet = xl.get_sheet(file_name=str(bloomberg_price_data), sheet_name="Data")
    except:
        user_logger.info('PRICEDATA ISSUE ~ Exiting Extract as unable to open price data file {}.'.format(str(bloomberg_price_data)))
        pd_file_open_error = 'Yes'
        exit()
    cmp_mktcap_headers = ['No', 'Code', 'Company', 'Sector', 'CMP', 'MKTCAP']
    cmp_mktcap_data = []
    annual_output['CMP & MKTCAP'] = [cmp_mktcap_headers]
    price_performance_headers = ['No', 'Code', 'Company', 'Sector', 'PP-1D', 'PP-5D', 'PP-1M', 'PP-3M', 'PP-6M', 'PP-1YR', 'PP-2YR', 'PP-3YR', 'PP-5YR', 'PP-7YR', 'PP-MTD', 'PP-YTD']
    price_performance_data = []
    annual_output['Price Performance (%)'] = [price_performance_headers]

    cmp_dict = []
    mktcap_dict = []
    rownum_cmp = 1
    for pd_index in price_data_sheet.rows():
        company_data = list(filter(lambda company: company['bloomberg_code'] == pd_index[1], active_companies_list))
        if company_data == []:
            continue
        code = ([code['code'] for code in company_data])[0]
        bloomberg_code = ([bloomberg_code['bloomberg_code'] for bloomberg_code in company_data])[0]
        company = ([company['name'] for company in company_data])[0]
        sector = ([sector['sector'] for sector in company_data])[0]
        cmp = pd_index[4]
        try:
            cmp = float(cmp)
        except:
            user_logger.info('CMP ~ Company {} within sector {} has no or invalid CMP data = {}'.format(code, sector, str(pd_index[4])))
            cmp = 0
        mktcap = pd_index[5]
        try:
            mktcap = float(mktcap)
        except:
            user_logger.info('MKTCAP ~ Compnay {} within sector {} has no or invalid MKTCAP data = {}'.format(code, sector, str(pd_index[5])))
            mktcap = 0
        cmp_dict.append({'bloomberg_code': bloomberg_code, 'cmp': cmp})
        mktcap_dict.append({'bloomberg_code': bloomberg_code, 'mktcap': mktcap})
        cmp_mktcap_data = [rownum_cmp, bloomberg_code, company, sector, cmp, mktcap]
        annual_output['CMP & MKTCAP'].append(cmp_mktcap_data)
        price_performance_data = [str(rownum_cmp), bloomberg_code, company, sector, pd_index[52], pd_index[53], pd_index[54], pd_index[55], pd_index[56], pd_index[57], pd_index[71], pd_index[72], pd_index[73], pd_index[74],  pd_index[58],  pd_index[59]]
        annual_output['Price Performance (%)'].append(price_performance_data)
        rownum_cmp += 1

    ########################### Extract all KPIs for each company #########################
    rownum_akpi = 0
    roce_slno = 0
    lacgr_slno = 0
    eveb_slno = 0
    for kpi_company in active_companies_list:
        admin_logger.info('ALL KPI MAIN LOOP ~ Started extraction of KPIs for {} within {}'.format(kpi_company['code'], kpi_company['sector']))
        data_file_open_error = 'No'
        data_file_xlsm = base_dir.joinpath('{}/models/{}.{}'.format(kpi_company['sector'], kpi_company['code'], 'xlsm'))
        data_file_xlsx = base_dir.joinpath('{}/models/{}.{}'.format(kpi_company['sector'], kpi_company['code'], 'xlsx'))
        if not path.exists(data_file_xlsm):
            if not path.exists(data_file_xlsx):
                continue
            else:
                try:
                    data_sheet = xl.get_sheet(file_name=str(data_file_xlsx), sheet_name='Annual')
                except:
                    data_file_open_error = 'Yes'
        else:
                try:
                    data_sheet = xl.get_sheet(file_name=str(data_file_xlsm), sheet_name='Annual')
                except:
                    data_file_open_error = 'Yes'
        if data_file_open_error == 'Yes':
            continue
        rownum_akpi += 1
        current_company_details = [rownum_akpi, kpi_company['bloomberg_code'], kpi_company['name'], kpi_company['sector']]
        current_company_header = []
        eps_dict = []
        bvps_dict = []
        ebitda_dict = []
        loansandadvances_dict = []
        netprofit_dict = []
        netsales_dict = []
        pbt_dict = []
        upside_dict = []
        got_dheaders = 'No'
        header_row_index  = 0
        data_col_map = []
        tp_col_map = []
        npcagr_dict = []
        per_dict = []
        cashbank_dict = []
        marketablesac_dict = []
        ltdebt_dict = []
        stdebt_dict = []
        deposits_dict = []
        borrowings_dict = []
        cash_dict = []
        totalincome_dict = []
        for kpi_index, kpi_row in enumerate(data_sheet.rows()):
            if kpi_row and kpi_row[3] == 'INR mn' and got_dheaders == 'No':
                admin_logger.info('      DATA COLUMN MAPPER ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                got_dheaders = 'Yes'
                header_row_index = kpi_index
                header_col_pos = 0
                for kpiheader_index in range(len(kpi_row)):
                    if kpiheader_index < 4 or data_sheet[kpi_index, kpiheader_index] == '':
                        continue
                    header_col_pos  = kpiheader_index
                    head_year = data_sheet[header_row_index, kpiheader_index]
                    year_is_mapped = 'No'
                    for year_variant in years_map:
                        if head_year in year_variant:
                            fyhd_year = str(year_variant[0])
                            year_is_mapped = 'Yes'
                            break
                        else:
                            fyhd_year = str('UMY_') + str(head_year)
                    if year_is_mapped == 'No':
                        user_logger.info('YEAR MAPPER ~ year {} in company {} within sector {} has no Year mapping in config file'.format(fyhd_year, kpi_company['code'], kpi_company['sector']))
                    data_col_map.append({'datacolposition': header_col_pos, 'datacolyear': fyhd_year, 'positioninmasterlist': headers_list.index(fyhd_year)})
                    if not head_year in current_company_header:
                        current_company_header.append(fyhd_year)
                data_col_map.append({'datacolposition': header_col_pos + 1, 'datacolyear': 'zotal', 'positioninmasterlist': headers_list.index('zotal')})
            elif kpi_row and kpi_row[3] == 'EPS (INR)':
                admin_logger.info('      EPS ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                eps_company_data = []
                if not annual_output.get('EPS'):
                    annual_output['EPS'] = [common_headers + ['EPS_'+ item for item in headers_list]]
                for eps_zero_item in headers_list:
                    eps_company_data.append(0)
                eps_total = 0
                for eps_curr_col in range(len(kpi_row)):
                    eps_col_data = 0
                    if eps_curr_col < 4 or data_sheet[kpi_index, eps_curr_col] == '':
                        continue
                    eps_col_data = data_sheet[kpi_index, eps_curr_col]
                    try:
                        eps_col_data = float(eps_col_data)
                    except:
                        eps_col_data = 0
                        eps_year = data_sheet[header_row_index, eps_curr_col]
                        user_logger.info('EPS ~ Company {} within Sector {} for year {} has no or invalid EPS data = {}'.format(kpi_company['code'], kpi_company['sector'], str(eps_year), str(eps_col_data)))
                    eps_total = eps_total + eps_col_data
                    for eps_map_item in data_col_map:
                        if eps_map_item['datacolposition'] == eps_curr_col:
                            eps_map_year = eps_map_item['datacolyear']
                            eps_company_data[eps_map_item['positioninmasterlist']] = eps_col_data
                            break
                    eps_dict.append({'positioninmasterlist': eps_map_item['positioninmasterlist'], 'eps_year': eps_map_year, 'eps_value': eps_col_data})
                eps_total_index = headers_list.index('zotal')
                eps_company_data[eps_total_index] = eps_total
                annual_output['EPS'].append(current_company_details + eps_company_data)
            elif kpi_row and kpi_row[3] == 'Net interest income (NII)':
                admin_logger.info('      NII ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                nii_company_data = []
                if not annual_output.get('Net Interest Income (NII)'):
                    annual_output['Net Interest Income (NII)'] = [common_headers + ['NII_'+ item for item in headers_list]]
                for nii_zero_item in headers_list:
                    nii_company_data.append(0)
                nii_total = 0
                for nii_curr_col in range(len(kpi_row)):
                    nii_col_data = 0
                    if nii_curr_col < 4 or data_sheet[kpi_index, nii_curr_col] == '':
                        continue
                    nii_col_data = data_sheet[kpi_index, nii_curr_col]
                    try:
                        nii_col_data = float(nii_col_data)
                    except:
                        nii_col_data = 0
                        nii_year = data_sheet[header_row_index, nii_curr_col]
                        user_logger.info('NII ~ Company {} within Sector {} for year {} has no or invalid NII data = {}'.format(kpi_company['code'], kpi_company['sector'], str(nii_year), str(nii_col_data)))
                    nii_total = nii_total + nii_col_data
                    for nii_map_item in data_col_map:
                        if nii_map_item['datacolposition'] == nii_curr_col:
                            nii_company_data[nii_map_item['positioninmasterlist']] = nii_col_data
                            break
                nii_total_index = headers_list.index('zotal')
                nii_company_data[nii_total_index] = nii_total
                annual_output['Net Interest Income (NII)'].append(current_company_details + nii_company_data)
            elif kpi_row and kpi_row[3] == 'Gross NPA (%)':
                admin_logger.info('      GROSS NPA ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                gnpa_company_data = []
                if not annual_output.get('Gross NPA (%)'):
                    annual_output['Gross NPA (%)'] = [common_headers + ['GNPA_'+ item for item in headers_list]]
                for gnpa_zero_item in headers_list:
                    gnpa_company_data.append(0)
                gnpa_total = 0
                for gnpa_curr_col in range(len(kpi_row)):
                    gnpa_col_data = 0
                    if gnpa_curr_col < 4 or data_sheet[kpi_index, gnpa_curr_col] == '':
                        continue
                    gnpa_col_data = data_sheet[kpi_index, gnpa_curr_col]
                    try:
                        gnpa_col_data = float(gnpa_col_data)
                    except:
                        gnpa_col_data = 0
                        gnpa_year = data_sheet[header_row_index, gnpa_curr_col]
                        user_logger.info('GROSS NPA ~ Company {} within Sector {} for year {} has no or invalid Gross NPA (%) data = {}'.format(kpi_company['code'], kpi_company['sector'], str(gnpa_year), str(gnpa_col_data)))
                    gnpa_total = gnpa_total + gnpa_col_data
                    for gnpa_map_item in data_col_map:
                        if gnpa_map_item['datacolposition'] == gnpa_curr_col:
                            gnpa_company_data[gnpa_map_item['positioninmasterlist']] = gnpa_col_data
                            break
                gnpa_total_index = headers_list.index('zotal')
                gnpa_company_data[gnpa_total_index] = gnpa_total
                annual_output['Gross NPA (%)'].append(current_company_details + gnpa_company_data)
            elif kpi_row and kpi_row[3] == 'Net NPA (%)':
                admin_logger.info('      NET NPA ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                nnpa_company_data = []
                if not annual_output.get('Net NPA (%)'):
                    annual_output['Net NPA (%)'] = [common_headers + ['NNPA_'+ item for item in headers_list]]
                for nnpa_zero_item in headers_list:
                    nnpa_company_data.append(0)
                nnpa_total = 0
                for nnpa_curr_col in range(len(kpi_row)):
                    nnpa_col_data = 0
                    if nnpa_curr_col < 4 or data_sheet[kpi_index, nnpa_curr_col] == '':
                        continue
                    nnpa_col_data = data_sheet[kpi_index, nnpa_curr_col]
                    try:
                        nnpa_col_data = float(nnpa_col_data)
                    except:
                        nnpa_col_data = 0
                        nnpa_year = data_sheet[header_row_index, nnpa_curr_col]
                        user_logger.info('NET NPA ~ Company {} within Sector {} for year {} has no or invalid Net NPA (%) data = {}'.format(kpi_company['code'], kpi_company['sector'], str(nnpa_year), str(nnpa_col_data)))
                    nnpa_total = nnpa_total + nnpa_col_data
                    for nnpa_map_item in data_col_map:
                        if nnpa_map_item['datacolposition'] == nnpa_curr_col:
                            nnpa_company_data[nnpa_map_item['positioninmasterlist']] = nnpa_col_data
                            break
                nnpa_total_index = headers_list.index('zotal')
                nnpa_company_data[nnpa_total_index] = nnpa_total
                annual_output['Net NPA (%)'].append(current_company_details + nnpa_company_data)
            elif kpi_row and kpi_row[3] == 'Net profit':
                admin_logger.info('      NET PROFIT ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                np_company_data = []
                if not annual_output.get('Net Profit'):
                    annual_output['Net Profit'] = [common_headers + ['NP_'+ item for item in headers_list]]
                for np_zero_item in headers_list:
                    np_company_data.append(0)
                np_total = 0
                for np_curr_col in range(len(kpi_row)):
                    np_col_data = 0
                    if np_curr_col < 4 or data_sheet[kpi_index, np_curr_col] == '':
                        continue
                    np_col_data = data_sheet[kpi_index, np_curr_col]
                    try:
                        np_col_data = float(np_col_data)
                    except:
                        np_col_data = 0
                        np_year = data_sheet[header_row_index, np_curr_col]
                        user_logger.info('NET PROFIT ~ Company {} within Sector {} for year {} has no or invalid Net Profit data = {}'.format(kpi_company['code'], kpi_company['sector'], str(np_year), str(np_col_data)))
                    np_total = np_total + np_col_data
                    for np_map_item in data_col_map:
                        if np_map_item['datacolposition'] == np_curr_col:
                            np_map_year = np_map_item['datacolyear']
                            np_company_data[np_map_item['positioninmasterlist']] = np_col_data
                            break

                    netprofit_dict.append({'positioninmasterlist': np_map_item['positioninmasterlist'], 'np_year': np_map_year, 'np_value': np_col_data})
                np_total_index = headers_list.index('zotal')
                np_company_data[np_total_index] = np_total
                annual_output['Net Profit'].append(current_company_details + np_company_data)
            elif kpi_row and kpi_row[3] == 'BVPS (INR)':
                admin_logger.info('      BVPS ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                bvps_company_data = []
                if not annual_output.get('BVPS'):
                    annual_output['BVPS'] = [common_headers + ['BVPS_'+ item for item in headers_list]]
                for bvps_zero_item in headers_list:
                    bvps_company_data.append(0)
                bvps_total = 0
                for bvps_curr_col in range(len(kpi_row)):
                    bvps_col_data = 0
                    if bvps_curr_col < 4 or data_sheet[kpi_index, bvps_curr_col] == '':
                        continue
                    bvps_col_data = data_sheet[kpi_index, bvps_curr_col]
                    try:
                        bvps_col_data = float(bvps_col_data)
                    except:
                        bvps_col_data = 0
                        bvps_year = data_sheet[header_row_index, bvps_curr_col]
                        user_logger.info('BVPS ~ Company {} within Sector {} for year {} has no or invalid BVPS data = {}'.format(kpi_company['code'], kpi_company['sector'], str(bvps_year), str(bvps_col_data)))
                    bvps_total = bvps_total + bvps_col_data
                    for bvps_map_item in data_col_map:
                        if bvps_map_item['datacolposition'] == bvps_curr_col:
                            bvps_map_year = bvps_map_item['datacolyear']
                            bvps_company_data[bvps_map_item['positioninmasterlist']] = bvps_col_data
                            break
                    bvps_dict.append({'positioninmasterlist': bvps_map_item['positioninmasterlist'], 'bvps_year': bvps_map_year, 'bvps_value': bvps_col_data})
                bvps_total_index = headers_list.index('zotal')
                bvps_company_data[bvps_total_index] = bvps_total
                annual_output['BVPS'].append(current_company_details + bvps_company_data)
            elif kpi_row and kpi_row[3] == 'ROE (%)':
                admin_logger.info('      ROE ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                roe_company_data = []
                if not annual_output.get('ROE (%)'):
                    annual_output['ROE (%)'] = [common_headers + ['ROE_'+ item for item in headers_list]]
                for roe_zero_item in headers_list:
                    roe_company_data.append(0)
                roe_total = 0
                for roe_curr_col in range(len(kpi_row)):
                    roe_col_data = 0
                    if roe_curr_col < 4 or data_sheet[kpi_index, roe_curr_col] == '':
                        continue
                    roe_col_data = data_sheet[kpi_index, roe_curr_col]
                    try:
                        roe_col_data = float(roe_col_data)
                    except:
                        roe_col_data = 0
                        data_year = data_sheet[header_row_index, roe_curr_col]
                        user_logger.info('ROE ~ Company {} within Sector {} for year {} has no or invalid ROE data = {}'.format(kpi_company['code'], kpi_company['sector'], str(data_year), str(roe_col_data)))
                    roe_total = roe_total + roe_col_data
                    for roe_map_item in data_col_map:
                        if roe_map_item['datacolposition'] == roe_curr_col:
                            roe_company_data[roe_map_item['positioninmasterlist']] = roe_col_data
                            break
                roe_total_index = headers_list.index('zotal')
                roe_company_data[roe_total_index] = roe_total
                annual_output['ROE (%)'].append(current_company_details + roe_company_data)
            elif kpi_row and kpi_row[3] == 'LLP/Avg Loan (%)':
                admin_logger.info('      LLP/AVG LOAN ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                llpal_company_data = []
                if not annual_output.get('LLP-Avg Loan (%)'):
                    annual_output['LLP-Avg Loan (%)'] = [common_headers + ['LLPAL_'+ item for item in headers_list]]
                for llpal_zero_item in headers_list:
                    llpal_company_data.append(0)
                llpal_total = 0
                for llpal_curr_col in range(len(kpi_row)):
                    llpal_col_data = 0
                    if llpal_curr_col < 4 or data_sheet[kpi_index, llpal_curr_col] == '':
                        continue
                    llpal_col_data = data_sheet[kpi_index, llpal_curr_col]
                    try:
                        llpal_col_data = float(llpal_col_data)
                    except:
                        llpal_col_data = 0
                        data_year = data_sheet[header_row_index, llpal_curr_col]
                        user_logger.info('LLP/AVG LOAN ~ Company {} within Sector {} for year {} has no or invalid LLP-Avg Loan (%) data = {}'.format(kpi_company['code'], kpi_company['sector'], str(data_year), str(llpal_col_data)))
                    llpal_total = llpal_total + llpal_col_data
                    for llpal_map_item in data_col_map:
                        if llpal_map_item['datacolposition'] == llpal_curr_col:
                            llpal_company_data[llpal_map_item['positioninmasterlist']] = llpal_col_data
                            break
                llpal_total_index = headers_list.index('zotal')
                llpal_company_data[llpal_total_index] = llpal_total
                annual_output['LLP-Avg Loan (%)'].append(current_company_details + llpal_company_data)
            elif kpi_row and kpi_row[3] == 'Pre-tax profit' and kpi_row[1] == 'pre_tax_profit_A':
                admin_logger.info('      PBT ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                pbt_company_data = []
                if not annual_output.get('PBT'):
                    annual_output['PBT'] = [common_headers + ['PBT_'+ item for item in headers_list]]
                for pbt_zero_item in headers_list:
                    pbt_company_data.append(0)
                pbt_total = 0
                for pbt_curr_col in range(len(kpi_row)):
                    pbt_col_data = 0
                    if pbt_curr_col < 4 or data_sheet[kpi_index, pbt_curr_col] == '':
                        continue
                    pbt_col_data = data_sheet[kpi_index, pbt_curr_col]
                    try:
                        pbt_col_data = float(pbt_col_data)
                    except:
                        pbt_col_data = 0
                        pbt_year = data_sheet[header_row_index, pbt_curr_col]
                        user_logger.info('PBT ~ Company {} within Sector {} for year {} has no or invalid Pre-Tax Profit data = {}'.format(kpi_company['code'], kpi_company['sector'], str(pbt_year), str(pbt_col_data)))
                    pbt_total = pbt_total + pbt_col_data
                    for pbt_map_item in data_col_map:
                        if pbt_map_item['datacolposition'] == pbt_curr_col:
                            pbt_map_year = pbt_map_item['datacolyear']
                            pbt_company_data[pbt_map_item['positioninmasterlist']] = pbt_col_data
                            break
                    pbt_dict.append({'positioninmasterlist': pbt_map_item['positioninmasterlist'], 'pbt_year': pbt_map_year, 'pbt_value': pbt_col_data})
                pbt_total_index = headers_list.index('zotal')
                pbt_company_data[pbt_total_index] = pbt_total
                annual_output['PBT'].append(current_company_details + pbt_company_data)
            elif kpi_row and (kpi_row[3] == 'Net sales' and kpi_company['sector'] != 'Financial Institutions') or (kpi_row[3] == 'Total income' and kpi_company['sector'] == 'Financial Institutions'):
                admin_logger.info('      NET SALES ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                ns_company_data = []
                if not annual_output.get('Net Sales'):
                    annual_output['Net Sales'] = [common_headers + ['NS_'+ item for item in headers_list]]
                for ns_zero_item in headers_list:
                    ns_company_data.append(0)
                ns_total = 0
                for ns_curr_col in range(len(kpi_row)):
                    ns_col_data = 0
                    if ns_curr_col < 4 or data_sheet[kpi_index, ns_curr_col] == '':
                        continue
                    ns_col_data = data_sheet[kpi_index, ns_curr_col]
                    try:
                        ns_col_data = float(ns_col_data)
                    except:
                        ns_col_data = 0
                        ns_year = data_sheet[header_row_index, ns_curr_col]
                        user_logger.info('NET SALES ~ Company {} within Sector {} for year {} has no or invalid Net Sales data = {}'.format(kpi_company['code'], kpi_company['sector'], str(ns_year), str(ns_col_data)))
                    ns_total = ns_total + ns_col_data
                    for ns_map_item in data_col_map:
                        if ns_map_item['datacolposition'] == ns_curr_col:
                            ns_map_year = ns_map_item['datacolyear']
                            ns_company_data[ns_map_item['positioninmasterlist']] = ns_col_data
                            break
                    netsales_dict.append({'positioninmasterlist': ns_map_item['positioninmasterlist'], 'ns_year': ns_map_year, 'ns_value': ns_col_data})
                    totalincome_dict.append({'positioninmasterlist': ns_map_item['positioninmasterlist'], 'totalincome_year': ns_map_year, 'totalincome_value': ns_col_data})
                ns_total_index = headers_list.index('zotal')
                ns_company_data[ns_total_index] = ns_total
                annual_output['Net Sales'].append(current_company_details + ns_company_data)
            elif kpi_row and (kpi_row[3] == 'EBITDA' and kpi_company['sector'] != 'Financial Institutions') or (kpi_row[3] == 'Pre-provisioning profit' and kpi_company['sector'] == 'Financial Institutions'):
                admin_logger.info('      EBITDA ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                eb_company_data = []
                if not annual_output.get('EBITDA'):
                    annual_output['EBITDA'] = [common_headers + ['EB_'+ item for item in headers_list]]
                for eb_zero_item in headers_list:
                    eb_company_data.append(0)
                eb_total = 0
                for eb_curr_col in range(len(kpi_row)):
                    eb_col_data = 0
                    if eb_curr_col < 4 or data_sheet[kpi_index, eb_curr_col] == '':
                        continue
                    eb_col_data = data_sheet[kpi_index, eb_curr_col]
                    try:
                        eb_col_data = float(eb_col_data)
                    except:
                        eb_col_data = 0
                        eb_year = data_sheet[header_row_index, eb_curr_col]
                        user_logger.info('EBITDA ~ Company {} within Sector {} for year {} has no or invalid EBITDA data = {}'.format(kpi_company['code'], kpi_company['sector'], str(eb_year), str(eb_col_data)))
                    eb_total = eb_total + eb_col_data
                    for eb_map_item in data_col_map:
                        if eb_map_item['datacolposition'] == eb_curr_col:
                            eb_map_year = eb_map_item['datacolyear']
                            eb_company_data[eb_map_item['positioninmasterlist']] = eb_col_data
                            break
                    ebitda_dict.append({'positioninmasterlist': eb_map_item['positioninmasterlist'], 'ebitda_year': eb_map_year, 'ebitda_value': eb_col_data})
                eb_total_index = headers_list.index('zotal')
                eb_company_data[eb_total_index] = eb_total
                annual_output['EBITDA'].append(current_company_details + eb_company_data)
            elif kpi_row and (kpi_row[3] == 'Yield (%)' and kpi_company['sector'] != 'Financial Institutions') or (kpi_row[3] == 'Dividend Yield (%)' and kpi_company['sector'] == 'Financial Institutions'):
                admin_logger.info('      YIELD ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                yld_company_data = []
                if not annual_output.get('Yield (%)'):
                    annual_output['Yield (%)'] = [common_headers + ['YLD_'+ item for item in headers_list]]
                for yld_zero_item in headers_list:
                    yld_company_data.append(0)
                yld_total = 0
                for yld_curr_col in range(len(kpi_row)):
                    yld_col_data = 0
                    if yld_curr_col < 4 or data_sheet[kpi_index, yld_curr_col] == '':
                        continue
                    yld_col_data = data_sheet[kpi_index, yld_curr_col]
                    try:
                        yld_col_data = float(yld_col_data)
                    except:
                        yld_col_data = 0
                        yld_year = data_sheet[header_row_index, yld_curr_col]
                        user_logger.info('YIELD ~ Company {} within Sector {} for year {} has no or invalid Yield (%) data = {}'.format(kpi_company['code'], kpi_company['sector'], str(yld_year), str(yld_col_data)))
                    yld_total = yld_total + yld_col_data
                    for yld_map_item in data_col_map:
                        if yld_map_item['datacolposition'] == yld_curr_col:
                            yld_company_data[yld_map_item['positioninmasterlist']] = yld_col_data
                            break
                yld_total_index = headers_list.index('zotal')
                yld_company_data[yld_total_index] = yld_total
                annual_output['Yield (%)'].append(current_company_details + yld_company_data)
            elif kpi_row and kpi_row[3] == 'ROCE (%)' and kpi_company['sector'] != 'Financial Institutions':
                admin_logger.info('      ROCE ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                roce_company_data = []
                if not annual_output.get('ROCE (%)'):
                    annual_output['ROCE (%)'] = [common_headers + ['ROCE_'+ item for item in headers_list]]
                for roce_zero_item in headers_list:
                    roce_company_data.append(0)
                roce_total = 0
                for roce_curr_col in range(len(kpi_row)):
                    roce_col_data = 0
                    if roce_curr_col < 4 or data_sheet[kpi_index, roce_curr_col] == '':
                        continue
                    roce_col_data = data_sheet[kpi_index, roce_curr_col]
                    try:
                        roce_col_data = float(roce_col_data)
                    except:
                        roce_col_data = 0
                        roce_year = data_sheet[header_row_index, roce_curr_col]
                        user_logger.info('ROCE ~ Company {} within Sector {} for year {} has no or invalid ROCE data = {}'.format(kpi_company['code'], kpi_company['sector'], str(roce_year), str(roce_col_data)))
                    roce_total = roce_total + roce_col_data
                    for roce_map_item in data_col_map:
                        if roce_map_item['datacolposition'] == roce_curr_col:
                            roce_company_data[roce_map_item['positioninmasterlist']] = roce_col_data
                            break
                roce_total_index = headers_list.index('zotal')
                roce_company_data[roce_total_index] = roce_total
                if kpi_company['sector'] != 'Financial Institutions':
                    roce_slno += 1
                    roce_company_header = [roce_slno, kpi_company['bloomberg_code'], kpi_company['name'], kpi_company['sector']]
                    annual_output['ROCE (%)'].append(roce_company_header + roce_company_data)
            elif kpi_row and kpi_row[3] == 'Cash & bank' and kpi_company['sector'] != 'Financial Institutions':
                for cb_curr_col in range(len(kpi_row)):
                    cb_col_data = 0
                    if cb_curr_col < 4 or data_sheet[kpi_index, cb_curr_col] == '':
                        continue
                    cb_col_data = data_sheet[kpi_index, cb_curr_col]
                    for cb_map_item in data_col_map:
                        if cb_map_item['datacolposition'] == cb_curr_col:
                            cb_map_year = cb_map_item['datacolyear']
                            break
                    cashbank_dict.append({'positioninmasterlist': cb_map_item['positioninmasterlist'], 'cb_year': cb_map_year, 'cb_value': cb_col_data})
            elif kpi_row and kpi_row[3] == 'Marketable securities at cost' and kpi_company['sector'] != 'Financial Institutions':
                for msac_curr_col in range(len(kpi_row)):
                    msac_col_data = 0
                    if msac_curr_col < 4 or data_sheet[kpi_index, msac_curr_col] == '':
                        continue
                    msac_col_data = data_sheet[kpi_index, msac_curr_col]
                    for msac_map_item in data_col_map:
                        if msac_map_item['datacolposition'] == msac_curr_col:
                            msac_map_year = msac_map_item['datacolyear']
                            break
                    marketablesac_dict.append({'positioninmasterlist': msac_map_item['positioninmasterlist'], 'msac_year': msac_map_year, 'msac_value': msac_col_data})
            elif kpi_row and kpi_row[3] == 'LT Debt' and kpi_company['sector'] != 'Financial Institutions':
                for ltd_curr_col in range(len(kpi_row)):
                    ltd_col_data = 0
                    if ltd_curr_col < 4 or data_sheet[kpi_index, ltd_curr_col] == '':
                        continue
                    ltd_col_data = data_sheet[kpi_index, ltd_curr_col]
                    for ltd_map_item in data_col_map:
                        if ltd_map_item['datacolposition'] == ltd_curr_col:
                            ltd_map_year = ltd_map_item['datacolyear']
                            break
                    ltdebt_dict.append({'positioninmasterlist': ltd_map_item['positioninmasterlist'], 'ltd_year': ltd_map_year, 'ltd_value': ltd_col_data})
            elif kpi_row and kpi_row[3] == 'ST Debt' and kpi_company['sector'] != 'Financial Institutions':
                for std_curr_col in range(len(kpi_row)):
                    std_col_data = 0
                    if std_curr_col < 4 or data_sheet[kpi_index, std_curr_col] == '':
                        continue
                    std_col_data = data_sheet[kpi_index, std_curr_col]
                    for std_map_item in data_col_map:
                        if std_map_item['datacolposition'] == std_curr_col:
                            std_map_year = std_map_item['datacolyear']
                            break
                    stdebt_dict.append({'positioninmasterlist': std_map_item['positioninmasterlist'], 'std_year': std_map_year, 'std_value': std_col_data})
            elif kpi_row and kpi_row[3] == 'Deposits' and kpi_company['sector'] == 'Financial Institutions':
                for dep_curr_col in range(len(kpi_row)):
                    dep_col_data = 0
                    if dep_curr_col < 4 or data_sheet[kpi_index, dep_curr_col] == '':
                        continue
                    dep_col_data = data_sheet[kpi_index, dep_curr_col]
                    for dep_map_item in data_col_map:
                        if dep_map_item['datacolposition'] == dep_curr_col:
                            dep_map_year = dep_map_item['datacolyear']
                            break
                    deposits_dict.append({'positioninmasterlist': dep_map_item['positioninmasterlist'], 'dep_year': dep_map_year, 'dep_value': dep_col_data})
            elif kpi_row and kpi_row[3] == 'Borrowings' and kpi_company['sector'] == 'Financial Institutions':
                for bor_curr_col in range(len(kpi_row)):
                    col_data = 0
                    if bor_curr_col < 4 or data_sheet[kpi_index, bor_curr_col] == '':
                        continue
                    bor_col_data = data_sheet[kpi_index, bor_curr_col]
                    for bor_map_item in data_col_map:
                        if bor_map_item['datacolposition'] == bor_curr_col:
                            bor_map_year = bor_map_item['datacolyear']
                            break
                    borrowings_dict.append({'positioninmasterlist': bor_map_item['positioninmasterlist'], 'bor_year': bor_map_year, 'bor_value': bor_col_data})
            elif kpi_row and kpi_row[3] == 'Cash' and kpi_company['sector'] == 'Financial Institutions':
                for cas_curr_col in range(len(kpi_row)):
                    cas_col_data = 0
                    if cas_curr_col < 4 or data_sheet[kpi_index, cas_curr_col] == '':
                        continue
                    cas_col_data = data_sheet[kpi_index, cas_curr_col]
                    for cas_map_item in data_col_map:
                        if cas_map_item['datacolposition'] == cas_curr_col:
                            cas_map_year = cas_map_item['datacolyear']
                            break
                    cash_dict.append({'positioninmasterlist': cas_map_item['positioninmasterlist'], 'cash_year': cas_map_year, 'cash_value': cas_col_data})
            elif kpi_row and kpi_row[3] == 'ROA (%)' and kpi_company['sector'] == 'Financial Institutions':
                admin_logger.info('      ROA ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                roa_company_data = []
                if not annual_output.get('ROA (%)'):
                    annual_output['ROA (%)'] = [common_headers + ['ROA_'+ item for item in headers_list]]
                for roa_zero_item in headers_list:
                    roa_company_data.append(0)
                roa_total = 0
                for roa_curr_col in range(len(kpi_row)):
                    roa_col_data = 0
                    if roa_curr_col < 4 or data_sheet[kpi_index, roa_curr_col] == '':
                        continue
                    roa_col_data = data_sheet[kpi_index, roa_curr_col]
                    try:
                        roa_col_data = float(roa_col_data)
                    except:
                        roa_year = data_sheet[header_row_index, roa_curr_col]
                        user_logger.info('ROA ~ Company {} within Sector {} for year {} has no or invalid ROA data = {}'.format(kpi_company['code'], kpi_company['sector'], str(roa_year), str(roa_col_data)))
                    roa_total = roa_total + roa_col_data
                    for roa_map_item in data_col_map:
                        if roa_map_item['datacolposition'] == roa_curr_col:
                            roa_company_data[roa_map_item['positioninmasterlist']] = roa_col_data
                            break
                roa_total_index = headers_list.index('zotal')
                roa_company_data[roa_total_index] = roa_total
                annual_output['ROA (%)'].append(current_company_details + roa_company_data) # Append this company eps data to final eps tab
            elif kpi_row and kpi_row[3] == 'Loans & advances' and kpi_company['sector'] == 'Financial Institutions':
                admin_logger.info('      LOANS & ADVANCES ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                la_company_data = []
                if not annual_output.get('Loans & Advances'):
                    annual_output['Loans & Advances'] = [common_headers + ['LA_'+ item for item in headers_list]]
                for la_zero_item in headers_list:
                    la_company_data.append(0)
                la_total = 0
                for la_curr_col in range(len(kpi_row)):
                    la_col_data = 0
                    if la_curr_col < 4 or data_sheet[kpi_index, la_curr_col] == '':
                        continue
                    la_col_data = data_sheet[kpi_index, la_curr_col]
                    try:
                        la_col_data = float(la_col_data)
                    except:
                        la_year = data_sheet[header_row_index, la_curr_col]
                        user_logger.info('LOANS & ADVANCES ~ Company {} within Sector {} for year {} has no or invalid Loans & Advances data = {}'.format(kpi_company['code'], kpi_company['sector'], str(la_year), str(la_col_data)))
                    la_total = la_total + la_col_data
                    for la_map_item in data_col_map:
                        if la_map_item['datacolposition'] == la_curr_col:
                            la_map_year = la_map_item['datacolyear']
                            la_company_data[la_map_item['positioninmasterlist']] = la_col_data
                            break
                    loansandadvances_dict.append({'positioninmasterlist': la_map_item['positioninmasterlist'], 'la_year': la_map_year, 'la_value': la_col_data})
                la_total_index = headers_list.index('zotal')
                la_company_data[la_total_index] = la_total
                annual_output['Loans & Advances'].append(current_company_details + la_company_data)
            elif kpi_row and kpi_row[3] == 'Net interest margin (%)' and kpi_company['sector'] == 'Financial Institutions':
                admin_logger.info('      NET INTEREST MARGIN ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                nim_company_data = []
                if not annual_output.get('Net Interest Margin (%)'):
                    annual_output['Net Interest Margin (%)'] = [common_headers + ['NIM_'+ item for item in headers_list]]
                for nim_zero_item in headers_list:
                    nim_company_data.append(0)
                nim_total = 0
                for nim_curr_col in range(len(kpi_row)):
                    nim_col_data = 0
                    if nim_curr_col < 4 or data_sheet[kpi_index, nim_curr_col] == '':
                        continue
                    nim_col_data = data_sheet[kpi_index, nim_curr_col]
                    try:
                        nim_col_data = float(nim_col_data)
                    except:
                        nim_year = data_sheet[header_row_index, nim_curr_col]
                        user_logger.info('NET INTEREST MARGIN ~ Company {} within Sector {} for year {} has no or invalid Net Interest Margin (%) data = {}'.format(kpi_company['code'], kpi_company['sector'], str(nim_year), str(nim_col_data)))
                    nim_total = nim_total + nim_col_data
                    for nim_map_item in data_col_map:
                        if nim_map_item['datacolposition'] == nim_curr_col:
                            nim_company_data[nim_map_item['positioninmasterlist']] = nim_col_data
                            break
                nim_total_index = headers_list.index('zotal')
                nim_company_data[nim_total_index] = nim_total
                annual_output['Net Interest Margin (%)'].append(current_company_details + nim_company_data)
            elif kpi_row and 'Price Target' in str(kpi_row[21]): # Target Price & Upside
                admin_logger.info('      Upside & Target Price ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
                targetprice_data = []
                upside_data = []
                tp_headers = ['Date', str(current_fy - 1), str(current_fy), str(current_fy + 1), str(current_fy + 2), str(current_fy + 3), 'Total']
                us_headers = ['Date', str(current_fy - 1), str(current_fy), str(current_fy + 1), str(current_fy + 2), str(current_fy + 3), 'Total']
                tp_pos = 0
                for tp_year in tp_headers:
                    tp_col_map.append({'datacolposition': tp_pos, 'datacolyear': tp_year})
                    tp_pos += 1
                if not annual_output.get('Target Price'):
                    annual_output['Target Price'] = [common_headers + ['TP_'+ item for item in tp_headers]]
                if not annual_output.get('Upside'):
                    annual_output['Upside'] = [common_headers + ['US_'+ item for item in us_headers]]
                for tp_zero in range(len(tp_headers)):
                    targetprice_data.append(0)
                for us_zero in range(len(us_headers)):
                    upside_data.append(0)
                us_cmp_val = 0
                for us_cmp_item in cmp_dict:
                    if kpi_company['bloomberg_code'] == us_cmp_item['bloomberg_code']:
                        us_cmp_val = us_cmp_item['cmp']
                        break
                targetprice_data[0] = tpdate
                upside_data[0] = tpdate
                tptotal = 0
                ustotal = 0
                for tp_row in range(1, 8):
                    upside_value = 0
                    tp_year = data_sheet[kpi_index + tp_row, 21]
                    tp_value = data_sheet[kpi_index + tp_row, 22]
                    if tp_value == '':
                        tp_value = 0
                    if tp_year == '':
                        tp_year = 'NoYear'
                    for year_variant in years_map:
                        if tp_year in year_variant:
                            mapped_year = str(year_variant[0])
                            if mapped_year in tp_headers:
                                for tp_item in tp_col_map:
                                    if str(tp_item['datacolyear']) == mapped_year:
                                        targetprice_data[tp_item['datacolposition']] = tp_value
                                        tptotal = tptotal + tp_value
                                        if tp_value != 0 and us_cmp_val != 0:
                                            try:
                                                upside_value = ((tp_value - us_cmp_val) / us_cmp_val) * 100
                                            except:
                                                user_logger.info('Upside ~ Unable to compute Upside for Company {} within Sector {} for current year {} cmp = {} Price Targets = {}'.format(kpi_company['code'], kpi_company['sector'], str(mapped_year), str(us_cmp_val), str(tp_value)))
                                                upside_value = 0
                                            upside_data[tp_item['datacolposition']] = upside_value
                                            ustotal = ustotal + upside_value
                                            break

                targetprice_data[6] = float(tptotal)
                upside_data[6] = float(ustotal)
                annual_output['Target Price'].append(current_company_details + targetprice_data)
                annual_output['Upside'].append(current_company_details + upside_data)

        ########################## Extract PER (x) KPI #########################
        admin_logger.info('      PER ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        per_company_data = []
        if not annual_output.get('PER (x)'):
            annual_output['PER (x)'] = [common_headers + ['PER_'+ item for item in headers_list]]
        for per_zero_item in headers_list:
            per_company_data.append(0)
        per_cmp_value = 0
        for per_cmp_item in cmp_dict:
            if kpi_company['bloomberg_code'] == per_cmp_item['bloomberg_code']:
                per_cmp_value = per_cmp_item['cmp']
                break
        per_total = 0
        for per_head_year in current_company_header:
            per_data_index = headers_list.index(per_head_year)
            per_eps_value = 0
            per_col_data = 0
            for per_eps_item in eps_dict:
                if per_eps_item['eps_year'] == per_head_year:
                    per_eps_value = per_eps_item['eps_value']
                    try:
                        per_col_data = per_cmp_value / per_eps_value
                    except:
                        user_logger.info('PER ~ Unable to compute PER (x) for Company {} within Sector {} for current year {} cmp = {} EPS = {}'.format(kpi_company['code'], kpi_company['sector'], str(per_head_year), str(per_cmp_value), str(per_eps_value)))
                    per_company_data[per_data_index] = per_col_data
                    per_total = per_total + per_col_data
                    break
            per_dict.append({'positioninmasterlist': per_data_index, 'per_year': per_head_year, 'per_value': per_col_data})
        per_total_index = headers_list.index('zotal')
        per_company_data[per_total_index] = per_total
        annual_output['PER (x)'].append(current_company_details + per_company_data)

        ########################## Extract PB (x) KPI #########################
        admin_logger.info('      PB ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        pb_company_data = []
        if not annual_output.get('PB (x)'):
            annual_output['PB (x)'] = [common_headers + ['PB_'+ item for item in headers_list]]
        for pb_zero_item in headers_list:
            pb_company_data.append(0)
        pb_cmp_value = 0
        for pb_cmp_item in cmp_dict:
            if kpi_company['bloomberg_code'] == pb_cmp_item['bloomberg_code']:
                pb_cmp_value = pb_cmp_item['cmp']
                break
        pb_total = 0
        for pb_head_year in current_company_header:
            pb_data_index = headers_list.index(pb_head_year)
            pb_col_data = 0
            for pb_bvps_item in bvps_dict:
                if pb_bvps_item['bvps_year'] == pb_head_year:
                    pb_bvps_value = pb_bvps_item['bvps_value']
                    try:
                        pb_col_data = pb_cmp_value / pb_bvps_value
                    except:
                        user_logger.info('PB ~ Unable to compute PB (x) for Company {} within Sector {} for current year {} cmp = {} BVPS = {}'.format(kpi_company['code'], kpi_company['sector'], str(pb_head_year), str(pb_cmp_val), str(pb_bvps_val)))
                    pb_company_data[pb_data_index] = pb_col_data
                    pb_total = pb_total + pb_col_data
                    break
        pb_total_index = headers_list.index('zotal')
        pb_company_data[pb_total_index] = pb_total
        annual_output['PB (x)'].append(current_company_details + pb_company_data)

        ########################## Extract EBITDA Growth (%) KPI #########################
        admin_logger.info('      EBITDA Growth ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        ebg_company_data = []
        if not annual_output.get('EBITDA Growth (%)'):
            annual_output['EBITDA Growth (%)'] = [common_headers + ['EBG_'+ item for item in headers_list]]
        for ebg_zero_item in headers_list:
            ebg_company_data.append(0)
        ebg_total = 0
        for ebg_head_year in current_company_header:
            if ebg_head_year == 'zotal':
                continue
            ebg_curr_year, ebg_prev_year, ebg_curr_value, ebg_prev_value, ebg_growth = 0, 0, 0, 0, 0
            ebg_error = 'No'
            ebg_data_index = headers_list.index(ebg_head_year)
            try:
                ebg_curr_year = int(ebg_head_year)
            except:
                ebg_error = 'Yes'
            try:
                ebg_prev_year = int(ebg_curr_year) - 1
            except:
                ebg_error = 'Yes'
            if ebg_error == 'No':
                for ebg_curr_val in ebitda_dict:
                    if ebg_curr_val['ebitda_year'] == str(ebg_curr_year):
                        ebg_curr_value = ebg_curr_val['ebitda_value']
                        break
                for ebg_prev_val in ebitda_dict:
                    if ebg_prev_val['ebitda_year'] == str(ebg_prev_year):
                        ebg_prev_value = ebg_prev_val['ebitda_value']
                        break
                if ebg_data_index != 0:
                    if ebg_curr_value != 0 and ebg_prev_value != 0:
                        try:
                            ebg_growth = ((ebg_curr_value - ebg_prev_value) / abs(ebg_prev_value)) * 100
                        except:
                            user_logger.info('EBITDA Growth ~ Unable to compute EBITDA Growth (%) for Company {} within Sector {} current year = {} EBITDA = {}, previous year = {} EBITDA = {} '.format(kpi_company['code'], kpi_company['sector'], str(ebg_head_year), str(ebg_curr_value), str(ebg_head_year), str(ebg_prev_value)))
                ebg_company_data[ebg_data_index] = ebg_growth
                ebg_total = ebg_total + ebg_growth
            else:
                user_logger.info('EBITDA Growth ~ Unable to compute EBITDA Growth (%) for Company {} within Sector {} current year = {} EBITDA = {}, previous year = {} EBITDA = {} '.format(kpi_company['code'], kpi_company['sector'], str(ebg_head_year), str(ebg_curr_value), str(ebg_head_year), str(ebg_prev_value)))
        ebg_total_index = headers_list.index('zotal')
        ebg_company_data[ebg_total_index] = ebg_total
        annual_output['EBITDA Growth (%)'].append(current_company_details + ebg_company_data)

        ########################## Extract Loans & Advances Growth (%) KPI #########################
        admin_logger.info('      LOANS & ADAVCES Growth ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        lag_company_data = []
        if not annual_output.get('LOANS & ADAVCES Growth (%)'):
            annual_output['LOANS & ADAVCES Growth (%)'] = [common_headers + ['LAG_'+ item for item in headers_list]]
        for lag_zero_item in headers_list:
            lag_company_data.append(0)
        lag_total = 0
        for lag_head_year in current_company_header:
            if lag_head_year == 'zotal':
                continue
            if kpi_company['sector'] != 'Financial Institutions':
                break
            lag_curr_year, lag_prev_year, lag_curr_value, lag_prev_value, lag_growth = 0, 0, 0, 0, 0
            lag_error = 'No'
            lag_data_index = headers_list.index(lag_head_year)
            try:
                lag_curr_year = int(lag_head_year)
            except:
                lag_error = 'Yes'
            try:
                lag_prev_year = int(lag_curr_year) - 1
            except:
                lag_error = 'Yes'
            if lag_error == 'No':
                for lag_curr_val in loansandadvances_dict:
                    if lag_curr_val['la_year'] == str(lag_curr_year):
                        lag_curr_value = lag_curr_val['la_value']
                        break
                for lag_prev_val in loansandadvances_dict:
                    if lag_prev_val['la_year'] == str(lag_prev_year):
                        lag_prev_value = lag_prev_val['la_value']
                        break
                if lag_data_index != 0:
                    if lag_curr_value != 0 and lag_prev_value != 0:
                        try:
                            lag_growth = ((lag_curr_value - lag_prev_value) / abs(lag_prev_value)) * 100
                        except:
                            user_logger.info('LOANS & ADAVCES Growth ~ Unable to compute LOANS & ADAVCES Growth (%) for Company {} within Sector {} current year = {} LOANS & ADAVCES = {}, previous year = {} LOANS & ADAVCES = {} '.format(kpi_company['code'], kpi_company['sector'], str(lag_head_year), str(lag_curr_value), str(lag_head_year), str(lag_prev_value)))
                lag_company_data[lag_data_index] = lag_growth
                lag_total = lag_total + lag_growth
            else:
                user_logger.info('LOANS & ADAVCES Growth ~ Unable to compute LOANS & ADAVCES Growth (%) for Company {} within Sector {} current year = {} LOANS & ADAVCES = {}, previous year = {} LOANS & ADAVCES = {} '.format(kpi_company['code'], kpi_company['sector'], str(lag_head_year), str(lag_curr_value), str(lag_head_year), str(lag_prev_value)))
        if kpi_company['sector'] == 'Financial Institutions':
            lag_total_index = headers_list.index('zotal')
            lag_company_data[lag_total_index] = lag_total
            annual_output['LOANS & ADAVCES Growth (%)'].append(current_company_details + lag_company_data)

        ########################## Extract Net Profit Growth (%) KPI #########################
        admin_logger.info('      Net Profit Growth ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        npg_company_data = []
        if not annual_output.get('Net Profit Growth (%)'):
            annual_output['Net Profit Growth (%)'] = [common_headers + ['NPG_'+ item for item in headers_list]]
        for npg_zero_item in headers_list:
            npg_company_data.append(0)
        npg_total = 0
        for npg_head_year in current_company_header:
            if npg_head_year == 'zotal':
                continue
            npg_curr_year, npg_prev_year, npg_curr_value, npg_prev_value, npg_growth = 0, 0, 0, 0, 0
            npg_error = 'No'
            npg_data_index = headers_list.index(npg_head_year)
            try:
                npg_curr_year = int(npg_head_year)
            except:
                npg_error = 'Yes'
            try:
                npg_prev_year = int(npg_curr_year) - 1
            except:
                npg_error = 'Yes'
            if npg_error == 'No':
                for npg_curr_val in netprofit_dict:
                    if npg_curr_val['np_year'] == str(npg_curr_year):
                        npg_curr_value = npg_curr_val['np_value']
                        break
                for npg_prev_val in netprofit_dict:
                    if npg_prev_val['np_year'] == str(npg_prev_year):
                        npg_prev_value = npg_prev_val['np_value']
                        break
                if npg_data_index != 0:
                    if npg_curr_value != 0 and npg_prev_value != 0:
                        try:
                            npg_growth = ((npg_curr_value - npg_prev_value) / abs(npg_prev_value)) * 100
                        except:
                            user_logger.info('Net Profit Growth ~ Unable to compute Net Profit Growth (%) for Company {} within Sector {} current year = {} Net Profit = {}, previous year = {} Net Profit = {} '.format(kpi_company['code'], kpi_company['sector'], str(npg_head_year), str(npg_curr_value), str(npg_head_year), str(npg_prev_value)))
                npg_total = npg_total + npg_growth
                npg_company_data[npg_data_index] = npg_growth
            else:
                user_logger.info('Net Profit Growth ~ Unable to compute EBITDA Growth (%) for Company {} within Sector {} current year = {} EBITDA = {}, previous year = {} EBITDA = {} '.format(kpi_company['code'], kpi_company['sector'], str(npg_head_year), str(npg_curr_value), str(npg_head_year), str(npg_prev_value)))
        npg_total_index = headers_list.index('zotal')
        npg_company_data[npg_total_index] = npg_total
        annual_output['Net Profit Growth (%)'].append(current_company_details + npg_company_data)

        ########################## Extract Net Profit Growth (%) KPI #########################
        admin_logger.info('      Net Sales Growth ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        nsg_company_data = []
        if not annual_output.get('Net Sales Growth (%)'):
            annual_output['Net Sales Growth (%)'] = [common_headers + ['NSG_' + item for item in headers_list]]
        for nsg_zero_item in headers_list:
            nsg_company_data.append(0)
        nsg_total = 0
        for nsg_head_year in current_company_header:
            if nsg_head_year == 'zotal':
                continue
            nsg_curr_year, nsg_prev_year, nsg_curr_value, nsg_prev_value, nsg_growth = 0, 0, 0, 0, 0
            nsg_error = 'No'
            nsg_data_index = headers_list.index(nsg_head_year)
            try:
                nsg_curr_year = int(nsg_head_year)
            except:
                nsg_error = 'Yes'
            try:
                nsg_prev_year = int(nsg_curr_year) - 1
            except:
                nsg_error = 'Yes'
            if nsg_error == 'No':
                for nsg_curr_val in netsales_dict:
                    if nsg_curr_val['ns_year'] == str(nsg_curr_year):
                        nsg_curr_value = nsg_curr_val['ns_value']
                        break
                for nsg_prev_val in netsales_dict:
                    if nsg_prev_val['ns_year'] == str(nsg_prev_year):
                        nsg_prev_value = nsg_prev_val['ns_value']
                        break
                if nsg_data_index != 0:
                    if nsg_curr_value != 0 and nsg_prev_value != 0:
                        try:
                            nsg_growth = ((nsg_curr_value - nsg_prev_value) / abs(nsg_prev_value)) * 100
                        except:
                            user_logger.info('Net Sales Growth ~ Unable to compute Net Sales Growth (%) for Company {} within Sector {} current year = {} Net Sales = {}, previous year = {} Net Sales = {} '.format(kpi_company['code'], kpi_company['sector'], str(nsg_head_year), str(nsg_curr_value), str(nsg_head_year), str(nsg_prev_value)))
                nsg_total = nsg_total + nsg_growth
                nsg_company_data[nsg_data_index] = nsg_growth
            else:
                user_logger.info('Net Sales Growth ~ Unable to compute Net Sales Growth (%) for Company {} within Sector {} current year = {} Net Sales = {}, previous year = {} Net Sales = {} '.format(kpi_company['code'], kpi_company['sector'], str(nsg_head_year), str(nsg_curr_value), str(nsg_head_year), str(nsg_prev_value)))
        nsg_total_index = headers_list.index('zotal')
        nsg_company_data[nsg_total_index] = nsg_total
        annual_output['Net Sales Growth (%)'].append(current_company_details + nsg_company_data)

        ########################## Extract PBT Growth (%) KPI #########################
        admin_logger.info('      PBT Growth ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        pbtg_company_data = []
        if not annual_output.get('PBT Growth (%)'):
            annual_output['PBT Growth (%)'] = [common_headers + ['PBTG_'+ item for item in headers_list]]
        for pbtg_zero_item in headers_list:
            pbtg_company_data.append(0)
        pbtg_total = 0
        for pbtg_head_year in current_company_header:
            if pbtg_head_year == 'zotal':
                continue
            pbtg_curr_year, pbtg_prev_year, pbtg_curr_value, pbtg_prev_value, pbtg_growth = 0, 0, 0, 0, 0
            pbtg_error = 'No'
            pbtg_data_index = headers_list.index(pbtg_head_year)
            try:
                pbtg_curr_year = int(pbtg_head_year)
            except:
                pbtg_error = 'Yes'
            try:
                pbtg_prev_year = int(pbtg_curr_year) - 1
            except:
                pbtg_error = 'Yes'
            if pbtg_error == 'No':
                for pbtg_curr_val in pbt_dict:
                    if pbtg_curr_val['pbt_year'] == str(pbtg_curr_year):
                        pbtg_curr_value = pbtg_curr_val['pbt_value']
                        break
                for pbtg_prev_val in pbt_dict:
                    if pbtg_prev_val['pbt_year'] == str(pbtg_prev_year):
                        pbtg_prev_value = pbtg_prev_val['pbt_value']
                        break
                if pbtg_data_index != 0:
                    if pbtg_curr_value != 0 and pbtg_prev_value != 0:
                        try:
                            pbtg_growth = ((pbtg_curr_value - pbtg_prev_value) / abs(pbtg_prev_value)) * 100
                        except:
                            user_logger.info('PBT Growth ~ Unable to compute PBT Growth (%) for Company {} within Sector {} current year = {} PBT = {}, previous year = {} PBT = {} '.format(kpi_company['code'], kpi_company['sector'], str(pbtg_head_year), str(pbtg_curr_value), str(pbtg_head_year), str(pbtg_prev_value)))
                pbtg_total = pbtg_total + pbtg_growth
                pbtg_company_data[pbtg_data_index] = pbtg_growth
            else:
                user_logger.info('PBT Growth ~ Unable to compute PBT Growth (%) for Company {} within Sector {} current year = {} PBT = {}, previous year = {} PBT = {} '.format(kpi_company['code'], kpi_company['sector'], str(pbtg_head_year), str(pbtg_curr_value), str(pbtg_head_year), str(pbtg_prev_value)))
        pbtg_total_index = headers_list.index('zotal')
        pbtg_company_data[pbtg_total_index] = pbtg_total
        annual_output['PBT Growth (%)'].append(current_company_details + pbtg_company_data)

        ########################## Extract EBITDA CAGR KPI #########################
        admin_logger.info('      EBITDA CAGR ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        eb_cagr_headers = ['EBCAGR_L5Y','EBCAGR_L3Y', 'EBCAGR_N2Y', 'EBCAGR-zotal']
        ebcgr_company_data = []
        ebcgr_yr_m5_year = current_fy - 5
        ebcgr_yr_m3_year = current_fy - 3
        ebcgr_yr_cu_year = current_fy
        ebcgr_yr_p2_year = current_fy + 2
        ebcgr_yr_m5_eb, ebcgr_yr_m3_eb, ebcgr_yr_cu_eb, ebcgr_yr_p2_eb = 0, 0, 0, 0
        ebcgr_yr_m5_ebcagr, ebcgr_yr_m3_ebcagr, ebcgr_yr_p2_ebcagr, ebcgr_total = 0, 0, 0, 0
        if not annual_output.get('EBITDA CAGR'):
            annual_output['EBITDA CAGR'] = [common_headers + eb_cagr_headers]
        for ebcgr_zero_item in eb_cagr_headers:
            ebcgr_company_data.append(0)
        for ebcgr_ebitda_item in ebitda_dict:
            if str(ebcgr_ebitda_item['ebitda_year']) == str(ebcgr_yr_m5_year):
                ebcgr_yr_m5_eb = ebcgr_ebitda_item['ebitda_value']
            elif str(ebcgr_ebitda_item['ebitda_year']) == str(ebcgr_yr_m3_year):
                ebcgr_yr_m3_eb = ebcgr_ebitda_item['ebitda_value']
            elif str(ebcgr_ebitda_item['ebitda_year']) == str(current_fy):
                ebcgr_yr_cu_eb = ebcgr_ebitda_item['ebitda_value']
            elif str(ebcgr_ebitda_item['ebitda_year']) == str(ebcgr_yr_p2_year):
                ebcgr_yr_p2_eb = ebcgr_ebitda_item['ebitda_value']
        if ebcgr_yr_cu_eb != 0 and ebcgr_yr_m5_eb != 0:
            try:
                ebcgr_yr_m5_ebcagr = ((math.pow((ebcgr_yr_cu_eb/abs(ebcgr_yr_m5_eb)), Decimal(1/5))) - 1) * 100
            except:
                user_logger.info('EBITDA CAGR ~ Unable to compute EBCAGR-Last 5Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(ebcgr_yr_cu_year), str(ebcgr_yr_cu_eb), str(ebcgr_yr_m5_year), str(ebcgr_yr_m5_eb)))
        if ebcgr_yr_cu_eb != 0 and ebcgr_yr_m3_eb != 0:
            try:
                ebcgr_yr_m3_ebcagr = ((math.pow((ebcgr_yr_cu_eb/abs(ebcgr_yr_m3_eb)), Decimal(1/3))) - 1) * 100
            except:
                user_logger.info('EBITDA CAGR ~ Unable to compute EBCAGR-Last 3Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(ebcgr_yr_cu_year), str(ebcgr_yr_cu_eb), str(ebcgr_yr_m3_year), str(ebcgr_yr_m3_eb)))
        if ebcgr_yr_p2_eb != 0 and ebcgr_yr_cu_eb != 0:
            try:
                ebcgr_yr_p2_ebcagr = ((math.pow((ebcgr_yr_p2_eb/abs(ebcgr_yr_cu_eb)), Decimal(1/2))) - 1) * 100
            except:
                user_logger.info('EBITDA CAGR ~ Unable to compute EBCAGR-Next 2Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(ebcgr_yr_cu_year), str(ebcgr_yr_cu_eb), str(ebcgr_yr_p2_year), str(ebcgr_yr_p2_eb)))
        ebcgr_total = ebcgr_yr_m5_ebcagr + ebcgr_yr_m3_ebcagr + ebcgr_yr_p2_ebcagr
        ebcgr_company_data[0] = ebcgr_yr_m5_ebcagr
        ebcgr_company_data[1] = ebcgr_yr_m3_ebcagr
        ebcgr_company_data[2] = ebcgr_yr_p2_ebcagr
        ebcgr_company_data[3] = ebcgr_total
        annual_output['EBITDA CAGR'].append(current_company_details + ebcgr_company_data)

        ########################## Extract NET SALES CAGR KPI #########################
        admin_logger.info('      NET SALES CAGR ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        ns_cagr_headers = ['NSCAGR_L5Y','NSCAGR_L3Y', 'NSCAGR_N2Y', 'NSCAGR_zotal']
        nscgr_company_data = []
        nscgr_yr_m5_year = current_fy - 5
        nscgr_yr_m3_year = current_fy - 3
        nscgr_yr_cu_year = current_fy
        nscgr_yr_p2_year = current_fy + 2
        nscgr_yr_m5_ns, nscgr_yr_m3_ns, nscgr_yr_cu_ns, nscgr_yr_p2_ns,  = 0, 0, 0, 0
        nscgr_yr_m5_nscagr, nscgr_yr_m3_nscagr, nscgr_yr_p2_nscagr, nscgr_total = 0, 0, 0, 0
        if not annual_output.get('Net Sales CAGR'):
            annual_output['Net Sales CAGR'] = [common_headers + ns_cagr_headers]
        for nscgr_zero_item in ns_cagr_headers:
            nscgr_company_data.append(0)
        for nscgr_ns_item in netsales_dict:
            if str(nscgr_ns_item['ns_year']) == str(nscgr_yr_m5_year):
                nscgr_yr_m5_ns = nscgr_ns_item['ns_value']
            elif str(nscgr_ns_item['ns_year']) == str(nscgr_yr_m3_year):
                nscgr_yr_m3_ns = nscgr_ns_item['ns_value']
            elif str(nscgr_ns_item['ns_year']) == str(current_fy):
                nscgr_yr_cu_ns = nscgr_ns_item['ns_value']
            elif str(nscgr_ns_item['ns_year']) == str(nscgr_yr_p2_year):
                nscgr_yr_p2_ns = nscgr_ns_item['ns_value']
        if nscgr_yr_cu_ns != 0 and nscgr_yr_m5_ns != 0:
            try:
                nscgr_yr_m5_nscagr = ((math.pow((nscgr_yr_cu_ns/abs(nscgr_yr_m5_ns)), Decimal(1/5))) - 1) * 100
            except:
                user_logger.info('NET SALES CAGR ~ Unable to compute NSCAGR-Last 5Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(nscgr_yr_cu_year), str(nscgr_yr_cu_ns), str(nscgr_yr_m5_year), str(nscgr_yr_m5_ns)))
        if nscgr_yr_cu_ns != 0 and nscgr_yr_m3_ns != 0:
            try:
                nscgr_yr_m3_nscagr = ((math.pow((nscgr_yr_cu_ns/abs(nscgr_yr_m3_ns)), Decimal(1/3))) - 1) * 100
            except:
                user_logger.info('NET SALES CAGR ~ Unable to compute NSCAGR-Last 3Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(nscgr_yr_cu_year), str(nscgr_yr_cu_ns), str(nscgr_yr_m3_year), str(nscgr_yr_m3_ns)))
        if nscgr_yr_p2_ns != 0 and nscgr_yr_cu_ns != 0:
            try:
                nscgr_yr_p2_nscagr = ((math.pow((nscgr_yr_p2_ns/abs(nscgr_yr_cu_ns)), Decimal(1/2))) - 1) * 100
            except:
                user_logger.info('NET SALES CAGR ~ Unable to compute NSCAGR-Next 2Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(nscgr_yr_cu_year), str(nscgr_yr_cu_ns), str(nscgr_yr_p2_year), str(nscgr_yr_p2_ns)))
        nscgr_total = nscgr_yr_m5_nscagr + nscgr_yr_m3_nscagr + nscgr_yr_p2_nscagr
        nscgr_company_data[0] = nscgr_yr_m5_nscagr
        nscgr_company_data[1] = nscgr_yr_m3_nscagr
        nscgr_company_data[2] = nscgr_yr_p2_nscagr
        nscgr_company_data[3] = nscgr_total
        annual_output['Net Sales CAGR'].append(current_company_details + nscgr_company_data)

        ########################## Extract LOANS & ADVANCES CAGR KPI #########################
        admin_logger.info('      LOANS & ADVANCES CAGR ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        la_cagr_headers = ['LACAGR_L5Y','LACAGR_L3Y', 'LACAGR_N2Y', 'LACAGR_zotal']
        lacgr_company_data = []
        lacgr_yr_m5_year = current_fy - 5
        lacgr_yr_m3_year = current_fy - 3
        lacgr_yr_cu_year = current_fy
        lacgr_yr_p2_year = current_fy + 2
        lacgr_yr_m5_la, lacgr_yr_m3_la, lacgr_yr_cu_la, lacgr_yr_p2_la = 0, 0, 0, 0
        lacgr_yr_m5_lacagr, lacgr_yr_m3_lacagr, lacgr_yr_p2_lacagr, lacgr_total = 0, 0, 0, 0
        if not annual_output.get('Loans & Advances CAGR'):
            annual_output['Loans & Advances CAGR'] = [common_headers + la_cagr_headers]
        for lacgr_zero_item in la_cagr_headers:
            lacgr_company_data.append(0)
        for lacgr_la_item in loansandadvances_dict:
            if kpi_company['sector'] != 'Financial Institutions':
                break
            else:
                if str(lacgr_la_item['la_year']) == str(lacgr_yr_m5_year):
                    lacgr_yr_m5_la = lacgr_la_item['la_value']
                elif str(lacgr_la_item['la_year']) == str(lacgr_yr_m3_year):
                    lacgr_yr_m3_la = lacgr_la_item['la_value']
                elif str(lacgr_la_item['la_year']) == str(current_fy):
                    lacgr_yr_cu_la = lacgr_la_item['la_value']
                elif str(lacgr_la_item['la_year']) == str(lacgr_yr_p2_year):
                    lacgr_yr_p2_la = lacgr_la_item['la_value']
        if lacgr_yr_cu_la != 0 and lacgr_yr_m5_la != 0:
            try:
                lacgr_yr_m5_lacagr = ((math.pow((lacgr_yr_cu_la/abs(lacgr_yr_m5_la)), Decimal(1/5))) - 1) * 100
            except:
                user_logger.info('LOANS & ADVANCES CAGR ~ Unable to compute LACAGR-Last 5Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(lacgr_yr_cu_year), str(lacgr_yr_cu_la), str(lacgr_yr_m5_year), str(lacgr_yr_m5_la)))
        if lacgr_yr_cu_la != 0 and lacgr_yr_m3_la != 0:
            try:
                lacgr_yr_m3_lacagr = ((math.pow((lacgr_yr_cu_la/abs(lacgr_yr_m3_la)), Decimal(1/3))) - 1) * 100
            except:
                user_logger.info('LOANS & ADVANCES CAGR ~ Unable to compute LACAGR-Last 3Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(lacgr_yr_cu_year), str(lacgr_yr_cu_la), str(lacgr_yr_m3_year), str(lacgr_yr_m3_la)))
        if lacgr_yr_p2_la != 0 and lacgr_yr_cu_la != 0:
            try:
                lacgr_yr_p2_lacagr = ((math.pow((lacgr_yr_p2_la/abs(lacgr_yr_cu_la)), Decimal(1/2))) - 1) * 100
            except:
                user_logger.info('LOANS & ADVANCES CAGR ~ Unable to compute LACAGR-Next 2Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(lacgr_yr_cu_year), str(lacgr_yr_cu_la), str(lacgr_yr_p2_year), str(lacgr_yr_p2_la)))
        lacgr_total = lacgr_yr_m5_lacagr + lacgr_yr_m3_lacagr + lacgr_yr_p2_lacagr
        lacgr_company_data[0] = lacgr_yr_m5_lacagr
        lacgr_company_data[1] = lacgr_yr_m3_lacagr
        lacgr_company_data[2] = lacgr_yr_p2_lacagr
        lacgr_company_data[3] = lacgr_total
        if kpi_company['sector'] == 'Financial Institutions':
            lacgr_slno += 1
            lacgr_company_header = [lacgr_slno, kpi_company['bloomberg_code'], kpi_company['name'], kpi_company['sector']]
            annual_output['Loans & Advances CAGR'].append(lacgr_company_header + lacgr_company_data)

        ########################## Extract NET PROFIT CAGR KPI #########################
        admin_logger.info('      Net Profit CAGR ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        np_cagr_headers = ['NPCAGR_L5Y','NPCAGR_L3Y', 'NPCAGR_N2Y', 'NPCAGR_zotal']
        npcgr_company_data = []
        npcgr_yr_m5_year = current_fy - 5
        npcgr_yr_m3_year = current_fy - 3
        npcgr_yr_cu_year = current_fy
        npcgr_yr_p2_year = current_fy + 2
        npcgr_yr_m5_np, npcgr_yr_m3_np, npcgr_yr_cu_np, npcgr_yr_p2_np = 0, 0, 0, 0
        npcgr_yr_m5_npcagr, npcgr_yr_m3_npcagr, npcgr_yr_p2_npcagr, npcagr_total = 0, 0, 0, 0
        if not annual_output.get('Net Profit CAGR'):
            annual_output['Net Profit CAGR'] = [common_headers + np_cagr_headers]
        for npcgr_zero_item in np_cagr_headers:
            npcgr_company_data.append(0)
        for npcgr_np_item in netprofit_dict:
            if str(npcgr_np_item['np_year']) == str(npcgr_yr_m5_year):
                npcgr_yr_m5_np = npcgr_np_item['np_value']
            elif str(npcgr_np_item['np_year']) == str(npcgr_yr_m3_year):
                npcgr_yr_m3_np = npcgr_np_item['np_value']
            elif str(npcgr_np_item['np_year']) == str(current_fy):
                npcgr_yr_cu_np = npcgr_np_item['np_value']
            elif str(npcgr_np_item['np_year']) == str(npcgr_yr_p2_year):
                npcgr_yr_p2_np = npcgr_np_item['np_value']
        if npcgr_yr_cu_np != 0 and npcgr_yr_m5_np != 0:
            try:
                npcgr_yr_m5_npcagr = ((math.pow((npcgr_yr_cu_np/abs(npcgr_yr_m5_np)), Decimal(1/5))) - 1) * 100
            except:
                user_logger.info('NET PROFIT CAGR ~ Unable to compute NPCAGR-Last 5Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(npcgr_yr_cu_year), str(npcgr_yr_cu_np), str(npcgr_yr_m5_year), str(npcgr_yr_m5_np)))
        if npcgr_yr_cu_np != 0 and npcgr_yr_m3_np != 0:
            try:
                npcgr_yr_m3_npcagr = ((math.pow((npcgr_yr_cu_np/abs(npcgr_yr_m3_np)), Decimal(1/3))) - 1) * 100
            except:
                user_logger.info('NET PROFIT CAGR ~ Unable to compute NPCAGR-Last 3Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(npcgr_yr_cu_year), str(npcgr_yr_cu_np), str(npcgr_yr_m3_year), str(npcgr_yr_m3_np)))
        if npcgr_yr_p2_np != 0 and npcgr_yr_cu_np != 0:
            try:
                npcgr_yr_p2_npcagr = ((math.pow((npcgr_yr_p2_np/abs(npcgr_yr_cu_np)), Decimal(1/2))) - 1) * 100
            except:
                user_logger.info('NET PROFIT CAGR ~ Unable to compute NPCAGR-Next 2Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(npcgr_yr_cu_year), str(npcgr_yr_cu_np), str(npcgr_yr_p2_year), str(npcgr_yr_p2_np)))
        npcagr_total = npcgr_yr_m5_npcagr + npcgr_yr_m3_npcagr + npcgr_yr_p2_npcagr
        npcgr_company_data[0] = npcgr_yr_m5_npcagr
        npcgr_company_data[1] = npcgr_yr_m3_npcagr
        npcgr_company_data[2] = npcgr_yr_p2_npcagr
        npcgr_company_data[3] = npcagr_total
        npcagr_dict.append({'npcagr_year': 'NPCAGR_L5Y', 'npcagr_value': npcgr_yr_m5_npcagr})
        npcagr_dict.append({'npcagr_year': 'NPCAGR_L3Y', 'npcagr_value': npcgr_yr_m3_npcagr})
        npcagr_dict.append({'npcagr_year': 'NPCAGR_N2Y', 'npcagr_value': npcgr_yr_p2_npcagr})
        annual_output['Net Profit CAGR'].append(current_company_details + npcgr_company_data)

        ########################## Extract PBT CAGR KPI #########################
        admin_logger.info('      PBT CAGR ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        pbt_cagr_headers = ['PBTCAGR_L5Y','PBTCAGR_L3Y', 'PBTCAGR_N2Y', 'PBTCAGR_zotal']
        pbtcgr_company_data = []
        pbtcgr_yr_m5_year = current_fy - 5
        pbtcgr_yr_m3_year = current_fy - 3
        pbtcgr_yr_cu_year = current_fy
        pbtcgr_yr_p2_year = current_fy + 2
        pbtcgr_yr_m5_pbt, pbtcgr_yr_m3_pbt, pbtcgr_yr_cu_pbt, pbtcgr_yr_p2_pbt = 0, 0, 0, 0
        pbtcgr_yr_m5_pbtcagr, pbtcgr_yr_m3_pbtcagr, pbtcgr_yr_p2_pbtcagr, pbtcgr_total = 0, 0, 0, 0
        if not annual_output.get('PBT CAGR'):
            annual_output['PBT CAGR'] = [common_headers + pbt_cagr_headers]
        for pbtcgr_zero_item in pbt_cagr_headers:
            pbtcgr_company_data.append(0)
        for pbtcgr_pbt_item in pbt_dict:
            if str(pbtcgr_pbt_item['pbt_year']) == str(pbtcgr_yr_m5_year):
                pbtcgr_yr_m5_pbt = pbtcgr_pbt_item['pbt_value']
            elif str(pbtcgr_pbt_item['pbt_year']) == str(pbtcgr_yr_m3_year):
                pbtcgr_yr_m3_pbt = pbtcgr_pbt_item['pbt_value']
            elif str(pbtcgr_pbt_item['pbt_year']) == str(current_fy):
                pbtcgr_yr_cu_pbt = pbtcgr_pbt_item['pbt_value']
            elif str(pbtcgr_pbt_item['pbt_year']) == str(pbtcgr_yr_p2_year):
                pbtcgr_yr_p2_pbt = pbtcgr_pbt_item['pbt_value']
        if pbtcgr_yr_cu_pbt != 0 and pbtcgr_yr_m5_pbt != 0:
            try:
                pbtcgr_yr_m5_pbtcagr = ((math.pow((pbtcgr_yr_cu_pbt/abs(pbtcgr_yr_m5_pbt)), Decimal(1/5))) - 1) * 100
            except:
                user_logger.info('PBT CAGR ~ Unable to compute PBTCAGR-Last 5Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(pbtcgr_yr_cu_year), str(pbtcgr_yr_cu_pbt), str(pbtcgr_yr_m5_year), str(pbtcgr_yr_m5_pbt)))
        if pbtcgr_yr_cu_pbt != 0 and pbtcgr_yr_m3_pbt != 0:
            try:
                pbtcgr_yr_m3_pbtcagr = ((math.pow((pbtcgr_yr_cu_pbt/abs(pbtcgr_yr_m3_pbt)), Decimal(1/3))) - 1) * 100
            except:
                user_logger.info('PBT CAGR ~ Unable to compute PBTCAGR-Last 3Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(pbtcgr_yr_cu_year), str(pbtcgr_yr_cu_pbt), str(pbtcgr_yr_m3_year), str(pbtcgr_yr_m3_pbt)))
        if pbtcgr_yr_p2_pbt != 0 and pbtcgr_yr_cu_pbt != 0:
            try:
                pbtcgr_yr_p2_pbtcagr = ((math.pow((pbtcgr_yr_p2_pbt/abs(pbtcgr_yr_cu_pbt)), Decimal(1/2))) - 1) * 100
            except:
                user_logger.info('PBT CAGR ~ Unable to compute PBTCAGR-Next 2Y for Company {} within Sector {} year = {} CAGR = {}, year = {} CAGR = {} '.format(kpi_company['code'], kpi_company['sector'], str(pbtcgr_yr_cu_year), str(pbtcgr_yr_cu_pbt), str(pbtcgr_yr_p2_year), str(pbtcgr_yr_p2_pbt)))
        pbtcgr_total = pbtcgr_yr_m5_pbtcagr + pbtcgr_yr_m3_pbtcagr + pbtcgr_yr_p2_pbtcagr
        pbtcgr_company_data[0] = pbtcgr_yr_m5_pbtcagr
        pbtcgr_company_data[1] = pbtcgr_yr_m3_pbtcagr
        pbtcgr_company_data[2] = pbtcgr_yr_p2_pbtcagr
        pbtcgr_company_data[3] = pbtcgr_total
        annual_output['PBT CAGR'].append(current_company_details + pbtcgr_company_data)

        ######################### Extract PEG KPI #########################
        admin_logger.info('      PEG ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        peg_headers = ['PEG']
        peg_company_data = []
        if not annual_output.get('PEG'):
            annual_output['PEG'] = [common_headers + peg_headers]
        peg_per_value, peg_npcagr2y_value, peg_value = 0, 0, 0
        for peg_per_item in per_dict:
            if str(peg_per_item['per_year']) == str(current_fy):
                peg_per_value = peg_per_item['per_value']
        for peg_npcagr_item in npcagr_dict:
            if peg_npcagr_item['npcagr_year'] == 'NPCAGR_N2Y':
                peg_npcagr2y_value = peg_npcagr_item['npcagr_value']
        try:
            peg_value = peg_per_value / peg_npcagr2y_value
        except:
            user_logger.info('PEG ~ Unable to compute PEG for Company {} within Sector {} PER Curr Year = {}, Net Profit CAGR 2Y = {}'.format(kpi_company['code'], kpi_company['sector'], str(peg_per_value), str(peg_npcagr2y_value)))
        peg_company_data.append(peg_value)
        annual_output['PEG'].append(current_company_details + peg_company_data)

        ######################### EV EBITDA KPI #########################
        admin_logger.info('      EVEB ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        eveb_company_data = []
        if not annual_output.get('EV EBITDA (x)'):
            annual_output['EV EBITDA (x)'] = [common_headers + ['EVEB_'+ item for item in headers_list]]
        for eveb_zero_item in headers_list:
            eveb_company_data.append(0)
        eveb_mktcap_value = 0
        for eveb_mktcap_item in mktcap_dict:
            if kpi_company['bloomberg_code'] == eveb_mktcap_item['bloomberg_code']:
                eveb_mktcap_value = eveb_mktcap_item['mktcap']
                break
        eveb_total = 0
        for eveb_head_year in current_company_header:
            if eveb_head_year == 'zotal':
                continue
            eveb_value = 0
            eveb_data_index = headers_list.index(eveb_head_year)
            if kpi_company['sector'] == 'Financial Institutions':
                break
            else:
                eveb_cb_value = 0
                for eveb_cb_item in cashbank_dict:
                    if str(eveb_cb_item['cb_year']) == eveb_head_year:
                        eveb_cb_value = eveb_cb_item['cb_value']
                        break
                eveb_msac_value = 0
                for eveb_msac_item in marketablesac_dict:
                    if str(eveb_msac_item['msac_year']) == eveb_head_year:
                        eveb_msac_value = eveb_msac_item['msac_value']
                        break
                eveb_ltd_value = 0
                for eveb_ltd_item in ltdebt_dict:
                    if str(eveb_ltd_item['ltd_year']) == eveb_head_year:
                        eveb_ltd_value = eveb_ltd_item['ltd_value']
                        break
                eveb_std_value = 0
                for eveb_std_item in stdebt_dict:
                    if str(eveb_std_item['std_year']) == eveb_head_year:
                        eveb_std_value = eveb_std_item['std_value']
                        break
                eveb_eb_value = 0
                for eveb_eb_item in ebitda_dict:
                    if eveb_eb_item['ebitda_year'] == str(eveb_head_year):
                        eveb_eb_value = eveb_eb_item['ebitda_value']
                        break
                try:
                    eveb_value = (eveb_mktcap_value - ((eveb_cb_value + eveb_msac_value) - (eveb_ltd_value + eveb_std_value))) / eveb_eb_value
                except:
                    user_logger.info('EVEB ~ Unable to compute EV EBITDA for Company {} within Sector {} for Year = {}, MKTCAP = {} Cash Bank = {} Marketable Securities = {} Long Term Debt = {} Short Term Debt = {} EBITDA = {} '.format(kpi_company['code'], kpi_company['sector'], str(eveb_head_year), str(eveb_mktcap_value), str(eveb_cb_value), str(eveb_msac_value), str(eveb_ltd_value), str(eveb_std_value), str(eveb_eb_value)))
                eveb_company_data[eveb_data_index] = eveb_value
                eveb_total = eveb_total + eveb_value
        eveb_total_index = headers_list.index('zotal')
        eveb_company_data[eveb_total_index] = eveb_total
        if kpi_company['sector'] != 'Financial Institutions':
            eveb_slno += 1
            eveb_company_header = [eveb_slno, kpi_company['bloomberg_code'], kpi_company['name'], kpi_company['sector']]
            annual_output['EV EBITDA (x)'].append(eveb_company_header + eveb_company_data)

        ######################### EV SALES KPI #########################
        admin_logger.info('      EVSL ~ Extraction for company {} within sector {}'.format(kpi_company['code'], kpi_company['sector']))
        evsl_company_data = []
        if not annual_output.get('EV SALES (x)'):
            annual_output['EV SALES (x)'] = [common_headers + ['EVSL_'+ item for item in headers_list]]
        for evsl_zero_item in headers_list:
            evsl_company_data.append(0)
        evsl_mktcap_value = 0
        for evsl_mktcap_item in mktcap_dict:
            if kpi_company['bloomberg_code'] == evsl_mktcap_item['bloomberg_code']:
                evsl_mktcap_value = evsl_mktcap_item['mktcap']
                break
        evsl_total = 0
        for evsl_head_year in current_company_header:
            if evsl_head_year == 'zotal':
                continue
            evsl_value = 0
            evsl_data_index = headers_list.index(evsl_head_year)
            if kpi_company['sector'] == 'Financial Institutions':
                evsl_cash_value = 0
                for evsl_cash_item in cash_dict:
                    if str(evsl_cash_item['cash_year']) == evsl_head_year:
                        evsl_cash_value = evsl_cash_item['cash_value']
                        break
                evsl_ti_value = 0
                for evsl_ti_item in totalincome_dict:
                    if str(evsl_ti_item['totalincome_year']) == evsl_head_year:
                        evsl_ti_value = evsl_ti_item['totalincome_value']
                        break
                if kpi_company['isbank'] == 1:
                    evsl_debt_value = 0
                    for evsl_debt_item in deposits_dict:
                        if str(evsl_debt_item['dep_year']) == evsl_head_year:
                            evsl_debt_value = evsl_debt_item['dep_value']
                            break
                elif kpi_company['isbank'] == 0:
                    evsl_debt_value = 0
                    for evsl_debt_item in borrowings_dict:
                        if str(evsl_debt_item['bor_year']) == evsl_head_year:
                            evsl_debt_value = evsl_debt_item['bor_value']
                            break
                try:
                    evsl_value = ((evsl_mktcap_value + evsl_debt_value) - evsl_cash_value) / evsl_ti_value
                except:
                    user_logger.info('EVSL ~ Unable to compute EV SALES for Company {} within Sector {} EV SALES Curr Year = {}, MKTCAP = {} Debt = {} Cash = {} Total Income = {}'.format(kpi_company['code'], kpi_company['sector'], str(evsl_head_year), str(evsl_mktcap_value), str(evsl_debt_value), str(evsl_cash_value), str(evsl_ti_value)))
                evsl_company_data[evsl_data_index] = evsl_value
                evsl_total = evsl_total + evsl_value
            else:
                evsl_cb_value = 0
                for evsl_cb_item in cashbank_dict:
                    if str(evsl_cb_item['cb_year']) == evsl_head_year:
                        evsl_cb_value = evsl_cb_item['cb_value']
                        break
                evsl_msac_value = 0
                for evsl_msac_item in marketablesac_dict:
                    if str(evsl_msac_item['msac_year']) == evsl_head_year:
                        evsl_msac_value = evsl_msac_item['msac_value']
                        break
                evsl_ltd_value = 0
                for evsl_ltd_item in ltdebt_dict:
                    if str(evsl_ltd_item['ltd_year']) == evsl_head_year:
                        evsl_ltd_value = evsl_ltd_item['ltd_value']
                        break
                evsl_std_value = 0
                for evsl_std_item in stdebt_dict:
                    if str(evsl_std_item['std_year']) == evsl_head_year:
                        evsl_std_value = evsl_std_item['std_value']
                        break
                evsl_ns_value = 0
                for evsl_ns_item in netsales_dict:
                    if evsl_ns_item['ns_year'] == str(evsl_head_year):
                        evsl_ns_value = evsl_ns_item['ns_value']
                        break
                try:
                    evsl_value = (evsl_mktcap_value - ((evsl_cb_value + evsl_msac_value) - (evsl_ltd_value + evsl_std_value))) / evsl_ns_value
                except:
                    user_logger.info('EVSL ~ Unable to compute EV SALES for Company {} within Sector {} EV SALES Curr Year = {}, MKTCAP = {} Cash Bank = {} Marketable Securities = {} Long Term Debt = {} Short Term Debt = {} Net Sales = {} '.format(kpi_company['code'], kpi_company['sector'], str(evsl_head_year), str(evsl_mktcap_value), str(evsl_cb_value), str(evsl_msac_value), str(evsl_ltd_value), str(evsl_std_value), str(evsl_ns_value)))
                evsl_company_data[evsl_data_index] = evsl_value
                evsl_total = evsl_total + evsl_value
        evsl_total_index = headers_list.index('zotal')
        evsl_company_data[evsl_total_index] = evsl_total
        annual_output['EV SALES (x)'].append(current_company_details + evsl_company_data)

    book = Book(annual_output)
    book.save_as(annual_file)
    book.save_as(date_annual_file)

    ######################### Extract history into history.xlsx #########################
    admin_logger.info('*** Started history file extraction ***')
    annual_file = '{}/annual.xlsx'.format("annual_data")
    history_file = '{}/history.xlsx'.format("annual_data")
    date_history_file = '{}/{}_history.xlsx'.format("annual_data",date.today().strftime("%Y%m%d"))

    history_output = {}
    annual_file_exists = 'Yes'

    try:
        annual_book = xl.get_book(file_name=annual_file)
    except:
        admin_logger.info(' Unable to extract history as annual.xlsx is not found in path ' + annual_data)
        exit()

    for annual_sheet in annual_book.sheet_names():
        current_sheet = annual_book.sheet_by_name(annual_sheet)
        if annual_sheet == 'Target Price':
            try:
                tph_sheet = xl.get_sheet(file_name=history_file, sheet_name="Target Price History")
            except:
                tp_current = [tp_index for tp_index in current_sheet.rows()]
                if not history_output.get('Target Price History'):
                    history_output['Target Price History'] = []
                history_output['Target Price History'] = tp_current
                break
            tph_original = [tph_index for tph_index in tph_sheet.rows()]
            try:
                tph_reverse = tph_original[::-1]
            except:
                tph_reverse = tph_original

            for tp_index, tp_row in enumerate(current_sheet.rows()):
                tp_company = tp_row[1]
                try:
                    tp_total = round(tp_row[10], 2)
                except:
                    tp_total = 0
                for tph_row in tph_reverse:
                    if tp_company in tph_row:
                        try:
                            tph_total = round(tph_row[10], 2)
                        except:
                            tph_total = 0
                        break
                if tp_total != tph_total:
                    tph_original.append(tp_row)
            sort_id = 0
            for tph_sort in tph_original:
                if sort_id == 0:
                    tph_sort[0] = 'No'
                else:
                    tph_sort[0] = sort_id
                sort_id += 1
            history_output['Target Price History'] = tph_original

    book = Book(history_output)
    book.save_as(history_file)
    book.save_as(date_history_file)

    ######################### Exract History Into Access  #########################
    admin_logger.info('*** Started history access extraction ***')
    access_database = '{}/history.accdb'.format("annual_data")

    conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=E:\yeddu\annual_data\history.accdb', autocommit=True)
    cursor = conn.cursor()

    try:
        access_book = xl.get_book(file_name=annual_file)
    except:
        admin_logger.info(' Unable to extract history as annual.xlsx is not found in path : ' + annual_data)
        exit()

    for access_sheet in access_book.sheet_names():
        curacc_sheet = access_book.sheet_by_name(access_sheet)
        if access_sheet == 'Target Price':
            lcount = 0
            first_time_sql = "SELECT count(*) FROM target_price_history"
            cursor.execute(first_time_sql)
            lcount = cursor.fetchone()
            if lcount[0] == 0:
                access_no = 0
                for access_index, access_row in enumerate(curacc_sheet.rows()):
                    if access_index == 0:
                        continue
                    access_no += 1
                    insert_sql = "INSERT INTO target_price_history(No, Code, Company, Sector, TP_Date, TP_2019, TP_2020, TP_2021, TP_2022, TP_2023, TP_Total) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    params = (access_no, access_row[1], access_row[2],  access_row[3], access_row[4], access_row[5], access_row[6],  access_row[7], access_row[8], access_row[9],  access_row[10])
                    cursor.execute(insert_sql, params)
                    cursor.commit()
            else:
                inc_no = lcount[0]
                for acctp_index, acc_tp_row in enumerate(curacc_sheet.rows()):
                    if acctp_index == 0:
                        continue
                    try:
                        acc_tp_company = acc_tp_row[1]
                        acc_tp_total = round(acc_tp_row[10], 2)
                    except:
                        acc_tp_total = 0
                    check_tp_sql = "SELECT TP_Total FROM target_price_history where code = ? ORDER BY TP_Date DESC, code DESC"
                    params = (acc_tp_company)
                    cursor.execute(check_tp_sql, params)
                    ldata = cursor.fetchone()
                    if ldata == None:
                        inc_no += 1
                        inc_insert_sql = "INSERT INTO target_price_history(No, Code, Company, Sector, TP_Date, TP_2019, TP_2020, TP_2021, TP_2022, TP_2023, TP_Total) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                        inc_params = (inc_no, acc_tp_row[1], acc_tp_row[2],  acc_tp_row[3], acc_tp_row[4], acc_tp_row[5], acc_tp_row[6],  acc_tp_row[7], acc_tp_row[8], acc_tp_row[9],  acc_tp_row[10])
                        cursor.execute(inc_insert_sql, inc_params)
                    else:
                        if round(ldata[0], 2) != acc_tp_total:
                            inc_no += 1
                            inc_insert_sql = "INSERT INTO target_price_history(No, Code, Company, Sector, TP_Date, TP_2019, TP_2020, TP_2021, TP_2022, TP_2023, TP_Total) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            inc_params = (inc_no, acc_tp_row[1], acc_tp_row[2],  acc_tp_row[3], acc_tp_row[4], acc_tp_row[5], acc_tp_row[6],  acc_tp_row[7], acc_tp_row[8], acc_tp_row[9],  acc_tp_row[10])
                            cursor.execute(inc_insert_sql, inc_params)
                cursor.commit()
    admin_logger.info('*** End of extract ***')
