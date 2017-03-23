import datetime,isodate
import sys,os
import pandas
#import pint
#UREG = pint.UnitRegistry()

class DataCurve(object): 
    """
    LoadCurve object, meant to harmonize from 
    IPKeys and CPR and Volttron itself. 


    Can be a curve of killowatt hours from the IPKeys FLAME, 
    or a curve of kwh from CPR, or who knows what. 

    """
    # Data curves that represent a snapshot at a point in time. 
    OBSERVATION = ("kW", "W", "V")
    # curves that represent an observation over an interval. 
    CUMULATIVE = ("kWH","WH")
    
    # sanity check the values.
    UNITS = {
        "kw":"kW",
        "w":"W",
        "v":"V",
        "kwh":"kWh",        
    }

    def __init__(self,values,duration=None,unit=None):
        self.values = values
        self.duration=duration
        self.unit=self.UNITS.get(unit.lower(),unit)
        
    @classmethod 
    def from_ipkeys (cls , values, **kwargs):
        """
        Assumes input values are of the form {dtstart: , duration, unit, value } 
        Validates two things from the in
        """

        unit = set( [v["unit"] for v in values])
        if  len(unit)!=1:
            raise ValueError( "Inconsistent units. We can't deal.")
        unit = v[0]["unit"]

        duration = ( v[0]["duration"] if len(set( [v["unit"] for v in values])) == 1 else None)
        
        values.sort(key=lambda x:x["dtstart"])
        if len(values) <= 1:
            raise ValueError( "Minimum length: 2")
        nxtdt = isodate.parse_datetime(values[0]["dtstart"]) + isodate.parse_duration(values[0]["duration"])
        for v in values[1:]:
            dt = isodate.parse_datetime(v["dtstart"])
            if nxtdt != v:
                raise ValueError( "Overlapping or gapping intervals at {}".format(v["dtstart"]))
            nxdt = dt + isodate.parse_duration(v["duration"])
                                        
        return cls(
            pandas.Series(dict([ (v["dtstart"],v["value"]) for v in values])),
            duration,
            unit)

    @classmethod
    def from_historian(cls,msg,**kwargs):
        """
        Presumes data either from the CPR agent or the 
        Volttron historian. 
       
        """
        unit = msg["metadata"]["units"]
        readings = msg["values"]
        # make sure they are in sorted order, don't assume. 
        readings.sort(key=lambda x:x[0])
        if len(readings) <= 1 :
            raise ValueError, "Minimum length: 2"
        duration = isodate.duration_isoformat(
            isodate.parse_datetime(readings[1][0])-
            isodate.parse_datetime(readings[0][0]))
        values = pandas.Series(dict(readings))
        return cls(values,duration, unit)

    def to_kW(self):
        """
        Convert a Watt-Hour curve into Watts, doing 
        the math. 
        """
        if self.unit != 'kWh':
            raise ValueError("Can't do this conversion yet: %s to kW"self.unit)
        if self.duration is None:
            raise ValueError("Can;t do it with non-uniformly-spaced curves yet")

        values = self.values * 3600 / isodate.parse_duration(self.duration).seconds
        return self.__class__(values, duration, "kW")

    def to_kWh(self):
        """
        Convert a Watt-Hour curve into Watts, doing 
        the math. 
        """
        if self.unit != 'kW':
            raise ValueError("Can't do this conversion yet: %s to kW"self.unit)
        if self.duration is None:
            raise ValueError("Can;t do it with non-uniformly-spaced curves yet")

        values = self.values * isodate.parse_duration(self.duration).seconds / 3600
        return self.__class__(values, duration, "kWh")
    

    def __neg__(self):
        return self.__class__(
            -self.values ,
            self.duration,
            self.unit)
        
    def __add__(self,other):
        if self.unit != other.unit:
            raise ValueError, "Units don't match"
        return self.__class__(
            self.values + other.values,
            None,
            self.unit)
    
    def __sub__(self,other):
        if self.unit != other.unit:
            raise ValueError, "Units don't match"
        return self.__class__(
            self.values - other.values,
            None,
            self.unit)
    
