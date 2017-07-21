from revoscalepy.computecontext.RxComputeContext import RxComputeContext
from revoscalepy.computecontext.RxInSqlServer import RxInSqlServer
# błąd imortu revoscalepy.computecontext.RxInSqlServer -> revoscalepy
# from revoscalepy.computecontext.RxInSqlServer import RxSqlServerData
from revoscalepy import RxSqlServerData 
from revoscalepy.etl.RxImport import rx_import #9.2.0 rx_import_datasource -> rx_import


class DataSource():

    def __init__(self, connectionstring):

         """Data source remote compute context


                Args:
                    connectionstring: connection string to the SQL server.
                    
            
          """
         self.__connectionstring = connectionstring
         
    

    def loaddata(self):
        #9.2.0 sqlQuery-> sql_query; connectionString-> connection_string
        #dataSource = RxSqlServerData(sqlQuery = "select * from dbo.trainingdata", verbose=True, reportProgress =True,
        #                             connectionString = self.__connectionstring) 
        dataSource = RxSqlServerData(sql_query = "select * from dbo.trainingdata", verbose=True, reportProgress =True,
                                     connection_string = self.__connectionstring)

        #9.2.0 connectionString->connection_string; autoCleanup-> auto_cleanup
        #self.__computeContext = RxInSqlServer(connectionString = self.__connectionstring, autoCleanup = True)
        self.__computeContext = RxInSqlServer(connection_string = self.__connectionstring, auto_cleanup = True)  
        #9.2.0 rx_import_datasource -> rx_import
        #data = rx_import_datasource(dataSource)
        data = rx_import(dataSource)

        return data

    def getcomputecontext(self):
 
        if self.__computeContext is None:
            raise RuntimeError("Data must be loaded before requesting computecontext!")

        return self.__computeContext


