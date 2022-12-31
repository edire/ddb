
import os
import clr


amo_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'MiscFiles', 'Microsoft.AnalysisServices.Tabular.DLL')
clr.AddReference(amo_path)
import Microsoft.AnalysisServices.Tabular as AMO


def ProcessTabular(db
                   , server=os.getenv('ssas_server')
                   , uid=os.getenv('ssas_uid')
                   , pwd=os.getenv('ssas_pwd')
                   , table=None
                   , partition=None):

    if server == 'localhost':
        conn = f'Provider=MSOLAP;Data Source={server};Initial Catalog={db}'
    else:
        conn = f'Provider=MSOLAP;Data Source={server};Initial Catalog={db}; User ID={uid}; Password={pwd}'

    AMOServer = AMO.Server()
    AMOServer.Connect(conn)
    db = AMOServer.Databases[db]

    refresh_item = db.Model
    if table != None:
        refresh_item = refresh_item.Tables.Find(table)
    if table != None and partition != None:
        refresh_item = refresh_item.Partitions.Find(partition)

    refresh_item.RequestRefresh(AMO.RefreshType.Full)
    op_result = db.Model.SaveChanges()
    AMOServer.Disconnect()

    if op_result.Impact.IsEmpty:
        raise Exception('Model was not processed!')