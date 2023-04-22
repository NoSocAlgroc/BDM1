from ops.Controller import Controller

from ops.landing.temporal.ingestTemporalLocal import ingestTemporalLocal
from ops.landing.temporal.ingestTemporalCollect import ingestTemporalCollect 
from ops.landing.persistent.ingestPersistent_idealista import ingestPersistent_idealista
from ops.landing.persistent.ingestPersistent_opendatabcn_income import ingestPersistent_opendatabcn_income
from ops.landing.persistent.ingestPersistent_opendatabcn_price import ingestPersistent_opendatabcn_price

controller=Controller(hdfsAddress="10.4.41.39",hdfsPort="9870",hdfsUser="bdm")
ingestPersistent_opendatabcn_price(controller,"2018_comp_vend_preu.csv")