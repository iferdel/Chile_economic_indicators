import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3

def cleaned_scrapped():
    con = sqlite3.connect('indicators.sqlite')
    data = pd.read_sql_query('SELECT * from scrapped', con)
    data = data.iloc[: , 1:] #remove first column as sql export creates an index automatically
    
    d = list((
            (enumerate(np.where(data.columns.str.contains('Estados Unidos'), 'IPC - EEUU', False))),
            (enumerate(np.where(data.columns.str.contains('Date'), 'Date', False))),
            (enumerate(np.where(data.columns.str.contains('Ñuble'), 'Unoccupied (%) - Ñuble region', False))),
            (enumerate(np.where(data.columns.str.contains('Antofagasta'), 'Unoccupied (%) - Antofagasta region', False))),
            (enumerate(np.where(data.columns.str.contains('dólar'), 'Exchange rate', False))),
            (enumerate(np.where(data.columns.str.contains('Nacional'), 'Unoccupied (%) - Nationally', False))),
            (enumerate(np.where(data.columns.str.contains('IPC'), 'IPC', False))),
            (enumerate(np.where(data.columns.str.contains('IMACEC'), 'Imacec rate', False))),
            (enumerate(np.where(data.columns.str.contains('cobre'), 'Copper (USD/Lbs)', False))),
     ))
    lst=[]
    for item in d:
        for key, value in item:
            if value!='False':
                lst.append((key, value))

    lst.sort(key=lambda tup: tup[0])
    data.dropna(inplace=True)   
    data.columns = [x[1] for x in lst]
    columnstofloat = data.columns[1:]
    data[columnstofloat] = data.loc[:,columnstofloat].astype('float64').apply(lambda x: round(x,1))
    data['Imacec rate'] = data.loc[:, 'Imacec rate'] - data.loc[:, 'Imacec rate'].shift(12)
    data = data.iloc[12:] #remove first year
    data.to_sql('cleaned_to_plot', con, if_exists='replace')
    con.close()
    return data

def main():  
    
    data = cleaned_scrapped()
    
    x = np.arange(0,len(data),1)
    fig, (ax1,ax2,ax3) = plt.subplots(3)
    fig.set_size_inches(18.5, 15.5)

    plt.style.use('seaborn')

    plt.setp([ax1,ax2,ax3], xticks=range(0,len(data),12), xticklabels=data.loc[:,'Date'][0:-1:12])

    ###########################################################################################################################
    #Unoccupied - Imacec
    ax1.fill_between(x,data.loc[:,'Unoccupied (%) - Nationally'], color="skyblue", alpha=0.4,label='Unoccupied Chilean people rate - Nationally')
    ax1.plot(x,data.loc[:,'Unoccupied (%) - Antofagasta region'],\
            color='steelblue', linestyle='dashed',label='Unoccupied Chilean people rate - Antofagasta region')
    ax1.plot(x,data.loc[:,'Unoccupied (%) - Ñuble region'],\
            color='steelblue', linestyle='dotted',label='Unoccupied Chilean people rate - Ñuble region')
    ax1.set_ylabel('Unoccupied people rate')
    ax1.grid(False)
    ax1.legend(loc='upper left')

    ax11 = ax1.twinx()
    ax11.plot(x,data.loc[:,'Imacec rate'],color='red', label = 'Imacec')
    ax11.set_ylabel('Imacec')
    ax11.grid(False)
    ax11.legend(loc='upper right')

    ax1.set_title('Anual unnocupied Chilean people rate in Antofagasta region and Monthly Index of Economic Activity (Imacec rate). Period: 2015 - 2022. Data from central bank of Chile')
    plt.tight_layout()

    ax1.axis([-0.5,len(data),0, data.loc[:,'Unoccupied (%) - Nationally'].max()*1.1])
    ax1.tick_params(axis='y', colors='steelblue')
    ax11.axis([-0.5,len(data), data.loc[:,'Imacec rate'].min()*1.2, data.loc[:,'Imacec rate'].max()*1.2])
    ax11.tick_params(axis='y', colors='red')

    ###########################################################################################################################
    #Exchange rate USD - Copper price

    # mask for line drawing between points ignoring missing data
    s1mask = np.isfinite(data.loc[:,'Exchange rate'])
    s2mask = np.isfinite(data.loc[:,'Copper (USD/Lbs)'])

    ax2.plot(x[s1mask],data.loc[:,'Exchange rate'][s1mask],color='skyblue', label='Exchange rate')
    ax2.grid(False)
    ax2.set_title('Anual Chilean currency exchange rate per USD and Copper value (USD/Lbs). Period: 2015 - 2022. Data from central bank of Chile')
    ax2.legend(loc='upper left')

    ax22 = ax2.twinx()
    ax22.plot(x[s2mask],data.loc[:,'Copper (USD/Lbs)'][s2mask],color='red', label= 'Copper ($USD/lb)')
    ax22.grid(False)


    plt.tight_layout()
    ax2.axis([-0.5,len(data),0, data.loc[:,'Exchange rate'].max()*1.1])
    ax22.axis([-0.5,len(data), data.loc[:,'Copper (USD/Lbs)'].min()*0.9, data.loc[:,'Copper (USD/Lbs)'].max()*1.2])


    ax2.tick_params(axis='y', colors='steelblue')
    ax22.tick_params(axis='y', colors='red')

    ax2.set_ylabel('USD to CLP exchange rate')
    ax22.set_ylabel('Price of refined copper BML (dollars / pound)')
    ax22.legend(loc='upper right')


    ###########################################################################################################################
    #IPC
    s3mask = np.isfinite(data.loc[:,'IPC'])
    s4mask = np.isfinite(data.loc[:,'IPC - EEUU'])

    ax3.plot(x[s3mask],data.loc[:,'IPC'][s3mask],color='skyblue', label= 'IPC - CHILE')
    ax3.plot(x[s4mask],data.loc[:,'IPC - EEUU'][s4mask],color='red', label= 'IPC EEUU')
    ax3.grid(False)

    plt.tight_layout()

    ax3.axis([-0.5,len(data), data.loc[:,['IPC', 'IPC - EEUU']].min(axis=1).min()*0.9, 
            data.loc[:,['IPC', 'IPC - EEUU']].max(axis=1).max()*1.2])

    ax3.tick_params(axis='y', colors='steelblue')
    ax3.set_ylabel('Consumer price index')
    ax3.legend(loc='upper left')

    plt.savefig('Imacec-unnocupied_exchangerate-Coppervalue.png')

if __name__=='__main__':
    main()