# -*- coding: utf-8 -*-
"""
Created on Tue Nov 20 18:55:27 2018

@author: pvsha
"""

import pandas as pd
import numpy as np
import pickle as pk

class DataHandling:    
    def load_data(self, filename):
        return pd.read_csv(filename, skip_blank_lines=True)
    
    def merge_data(self, filename1, filename2):
        print('Merging in progress.......')
        return pd.concat([self.load_data(filename1), self.load_data(filename2)],axis=0)
    
    def save_data(self,filename, filedata, columns_list):
        filedata.to_csv(filename,sep=',', index=None, columns=columns_list)
        print('Content saved to the file')

class DataCleaning:
    def __init__(self,dataframe):
        self.dataframe = dataframe
        self.support = 0
    
    def getProductCountbyOrder(self):
        grouped_data = self.dataframe.groupby('order_id')
        grouped_dataframe = grouped_data['product_id'].agg(['count']).reset_index()
        return grouped_dataframe
    
    def getOrderCountbyProduct(self):
        grouped_data = self.dataframe.groupby('product_id')
        grouped_dataframe = grouped_data['order_id'].agg(['count']).reset_index()
        return grouped_dataframe
    
    def removeOutliers(self):
        grouped_df = self.getProductCountbyOrder()
        IQR = grouped_df['count'].describe()['75%']-grouped_df['count'].describe()['25%']
        outlier_range = 1.5*IQR
        min_limit_outlier = int(outlier_range - grouped_df['count'].describe()['25%'])
        max_limit_outlier = int(outlier_range + grouped_df['count'].describe()['75%'])
        temp_df = grouped_df[grouped_df['count']>=min_limit_outlier]
        temp_df1 = grouped_df[grouped_df['count']<=max_limit_outlier]
        temp_df2 = pd.merge(temp_df, temp_df1, how='inner', on=['order_id', 'count'])
        self.dataframe = self.dataframe.loc[self.dataframe['order_id'].isin(temp_df2['order_id'])]
    
#    def GenerateInitialSupportFrame(self):
#        self.removeOutliers()
#        grouped_df = self.getOrderCountbyProduct()
#        #self.support = grouped_df['count'].median()
#        self.support = 500
#        temp_df = grouped_df[grouped_df['count']>=self.support]
#        self.dataframe = self.dataframe.loc[self.dataframe['product_id'].isin(temp_df['product_id'])]
#        return self.dataframe
    
    def GenerateTransactionList(self):
        self.removeOutliers()
        transaction_list = []
        order_id_list = self.dataframe['order_id'].unique().tolist()
        grouped_df = self.dataframe.groupby('order_id')
        for i  in order_id_list:
            transaction_list.append(grouped_df.get_group(i)['product_id'].tolist())
        with open('Data/instacart/TransactionList.pkl','wb') as fp:
            pk.dump(transaction_list,fp)
            
if __name__ == '__main__':
    #Place the order_products__prior.csv and  order_products__train.csv into  Data/instacart/
    datahandler = DataHandling()
    datahandler.save_data('Data/instacart/Merged_data.csv', 
                          datahandler.merge_data("Data/instacart/order_products__prior.csv",
                                                 "Data/instacart/order_products__train.csv"),
                                                 ['order_id', 'product_id'])
    df = datahandler.load_data('Data/instacart/Merged_data.csv')
    print(df.values[0,:].shape)
    dataanalyser = DataCleaning(df)
    dataanalyser.removeOutliers()
