from ops.Controller import Controller

from ops.landing.temporal.ingestTemporalLocal import ingestTemporalLocal
from ops.landing.temporal.ingestTemporalCollect import ingestTemporalCollect 
controller=Controller(hdfsAddress="10.4.41.39",hdfsPort="9870",hdfsUser="bdm")
ingestTemporalCollect(controller,"2013")