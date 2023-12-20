from data import fetch_new_data
from analysis import create_monthly_dataset
from output import main as omain
from alpaca_trade_api.rest import TimeFrame

plot = False
save = True
if __name__ == '__main__':
    test_syms = ['CVE']
    syms_energy = ['SU', 'CNQ', 'TRP', 'ENB']
    # list of TSX symbols by category
    syms_banks = ['BNS', 'TD', ]
    syms_tech = ['SHOP', 'BB', 'WEED', 'ACB', 'APHA', 'CRON']
    syms_comm = ['BCE', 'RCI', 'T', 'FNV', 'GOLD', 'ABX', 'NTR', 'AGU']
    syms_reits = ['REI.UN', 'HR.UN', 'CAR.UN', 'AP.UN', 'NVU.UN', 'BEI.UN', 'REF.UN', 'HR.UN', 'CAR.UN', 'AP.UN', 'NVU.UN', 'BEI.UN', 'REF.UN']
    syms_util = ['EMA', 'CU', 'FTS', 'AQN', 'RNW', 'BEP.UN', 'NPI', 'BLX', 'INE', 'IFC', 'POW', 'CPX', 'FTS', 'CU', 'EMA', 'AQN', 'RNW', 'BEP.UN', 'NPI', 'BLX', 'INE', 'IFC', 'POW', 'CPX']
    syms_telecom = ['RCI.B', 'T', 'BCE', 'SJR.B', 'QBR.B', 'CCA.TO', 'RCI.B', 'T', 'BCE', 'SJR.B', 'QBR.B', 'CCA.TO']
    syms_consumer = ['ATD.B', 'L', 'WN', 'MRU', 'EMP.A', 'ATD.B', 'L', 'WN', 'MRU', 'EMP.A']
    syms_industrials = ['CNR', 'CP', 'BBD.B', 'WSP', 'CAE', 'BBD.B', 'WSP', 'CAE']
    syms_materials = ['TECK.B', 'FM', 'TECK.B', 'FM']
    syms_health = ['WELL', 'WELL', 'CSU']
    syms_real_estate = ['REI.UN', 'HR.UN', 'CAR.UN', 'AP.UN', 'NVU.UN', 'BEI.UN', 'REF.UN', 'HR.UN', 'CAR.UN', 'AP.UN', 'NVU.UN', 'BEI.UN', 'REF.UN']
    syms_consumer_staples = ['ATD.B', 'L', 'WN', 'MRU', 'EMP.A', 'ATD.B', 'L', 'WN', 'MRU', 'EMP.A']
    syms_financials = ['BNS', 'TD', 'BMO', 'RY', 'CM', 'NA', 'BNS', 'TD', 'BMO', 'RY', 'CM', 'NA']
    syms = test_syms + syms_energy + syms_banks + syms_tech + syms_comm + syms_util + syms_telecom + syms_consumer + syms_industrials + syms_materials + syms_health + syms_real_estate + syms_consumer_staples + syms_financials
    
    syms = syms_tech
    for sym in syms:
        start_date = '2000-01-01'
        fetch_new_data(sym, start_date=start_date, tf=TimeFrame.Week)
        create_monthly_dataset(sym, plot, save)
        omain()