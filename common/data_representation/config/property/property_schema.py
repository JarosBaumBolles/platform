"""Property XML Schema"""
from dataclasses import dataclass

import yaml
from dataclass_factory import Factory

RAW_SCHEMA = """
  property:
    tag: 
      tag: hbd:property
    children_tags:
      tags:
        - espm:name
        - espm:primaryFunction
        - espm:grossFloorArea
        - espm:yearBuilt
        - espm:address
        - espm:numberOfBuildings
        - espm:isFederalProperty
        - espm:occupancyPercentage
        - espm:audit
        - hbd:propertyURI
        - hbd:datacenterOver75kW
        - hbd:propertyUses
        - hbd:meterPropertyAssociationList
        - hbd:tags
        - hbd:audit
    general_info:
      children_tags:
        tags:
          - espm:name
          - espm:primaryFunction
          - espm:grossFloorArea
          - espm:yearBuilt
          - espm:address
          - espm:numberOfBuildings
          - espm:isFederalProperty
          - espm:occupancyPercentage
          - espm:audit
          - hbd:propertyURI
          - hbd:datacenterOver75kW
      name:
        type: text
      primaryFunction:
        type: text
      grossFloorArea:
        type: floor_area
      yearBuilt:
        type: decimal
      address:
        type: address
      numberOfBuildings:
        type: decimal
      isFederalProperty:
        type: text
      occupancyPercentage:
        type: decimal
      propertyURI:
        type: text
      datacenterOver75kW:
        type: text
      audit:
        type: text

    property_uses:
      tag: 
        tag: hbd:propertyUses
      children_tags:
        tags:
          - hbd:office
          - hbd:parking
          - hbd:supermarket
          - hbd:retail
          - hbd:datacenter
          - hbd:k12School
          - hbd:cafeteria
      office:
        children_tags:
          tags:
            - hbd:useDetails
            - hbd:useTiming
        useDetails:
          tag: 
            tag: hbd:useDetails
          children_tags:
            tags:
              - espm:totalGrossFloorArea
              - espm:weeklyOperatingHours
              - espm:numberOfWorkers
              - espm:numberOfComputers
              - espm:percentOfficeCooled
              - espm:percentOfficeHeated
          totalGrossFloorArea:
            type: floor_area
          weeklyOperatingHours:
            type: property_decimal
          numberOfWorkers:
            type: property_decimal
          numberOfComputers:
            type: property_decimal
          percentOfficeCooled:
            type: property_text
          percentOfficeHeated:
            type: property_text
        useTiming:
          tag: 
            tag: hbd:useTiming
          children_tags:
            tags:
              - hbd:daily
              - hbd:weekly
          daily:
            type: use_timing_daily
          weekly:
            type: use_timing_weekly
      parking:
        children_tags:
          tags:
            - hbd:useDetails
            - hbd:useTiming
        useDetails:
          tag: 
            tag: hbd:useDetails
          children_tags:
            tags:
              - espm:supplementalHeating
              - espm:openFootage
              - espm:completelyEnclosedFootage
              - espm:partiallyEnclosedFootage
          supplementalHeating:
            type: property_text
          openFootage:
            type: floor_area
          completelyEnclosedFootage:
            type: floor_area
          partiallyEnclosedFootage:
            type: floor_area
        useTiming:
          tag: 
            tag: hbd:useTiming
          children_tags:
            tags:
              - hbd:daily
              - hbd:weekly
          daily:
            type: use_timing_daily
          weekly:
            type: use_timing_weekly
      supermarket:
        children_tags:
          tags:
            - hbd:useDetails
            - hbd:useTiming
        useDetails:
          tag: 
            tag: hbd:useDetails
          children_tags:
            tags:
              - espm:totalGrossFloorArea
              - espm:weeklyOperatingHours
              - espm:numberOfWorkers
              - espm:numberOfComputers
              - espm:numberOfCashRegisters
              - espm:numberOfWalkInRefrigerationUnits
              - espm:numberOfOpenClosedRefrigerationUnits
              - espm:percentCooled
              - espm:percentHeated
              - espm:singleStore
              - espm:exteriorEntranceToThePublic
              - espm:areaOfAllWalkInRefrigerationUnits
              - espm:lengthOfAllOpenClosedRefrigerationUnits
              - espm:cookingFacilities
          totalGrossFloorArea:
            type: floor_area
          weeklyOperatingHours:
            type: property_decimal
          numberOfWorkers:
            type: property_decimal
          numberOfComputers:
            type: property_decimal
          numberOfCashRegisters:
            type: property_decimal
          numberOfWalkInRefrigerationUnits:
            type: property_decimal
          numberOfOpenClosedRefrigerationUnits:
            type: property_decimal
          percentCooled:
            type: property_text
          percentHeated:
            type: property_text
          singleStore:
            type: property_text
          exteriorEntranceToThePublic:
            type: property_text
          areaOfAllWalkInRefrigerationUnits:
            type: floor_area
          lengthOfAllOpenClosedRefrigerationUnits:
            type: refrigeration_units
          cookingFacilities:
            type: property_text
        useTiming:
          tag: 
            tag: hbd:useTiming
          children_tags:
            tags:
              - hbd:daily
              - hbd:weekly
          daily:
            type: use_timing_daily
          weekly:
            type: use_timing_weekly
      retail:
        children_tags:
          tags:
            - hbd:useDetails
            - hbd:useTiming
        useDetails:
          tag: 
            tag: hbd:useDetails
          children_tags:
            tags:
              - espm:totalGrossFloorArea
              - espm:weeklyOperatingHours
              - espm:numberOfWorkers
              - espm:numberOfComputers
              - espm:numberOfCashRegisters
              - espm:numberOfWalkInRefrigerationUnits
              - espm:numberOfOpenClosedRefrigerationUnits
              - espm:percentCooled
              - espm:percentHeated
              - espm:singleStore
              - espm:exteriorEntranceToThePublic
              - espm:areaOfAllWalkInRefrigerationUnits
              - espm:lengthOfAllOpenClosedRefrigerationUnits
              - espm:cookingFacilities
          totalGrossFloorArea:
            type: floor_area
          weeklyOperatingHours:
            type: property_decimal
          numberOfWorkers:
            type: property_decimal
          numberOfComputers:
            type: property_decimal
          numberOfCashRegisters:
            type: property_decimal
          numberOfWalkInRefrigerationUnits:
            type: property_decimal
          numberOfOpenClosedRefrigerationUnits:
            type: property_decimal
          percentCooled:
            type: property_text
          percentHeated:
            type: property_text
          singleStore:
            type: property_text
          exteriorEntranceToThePublic:
            type: property_text
          areaOfAllWalkInRefrigerationUnits:
            type: floor_area
          lengthOfAllOpenClosedRefrigerationUnits:
            type: refrigeration_units
          cookingFacilities:
            type: property_text
        useTiming:
          tag: 
            tag: hbd:useTiming
          children_tags:
            tags:
              - hbd:daily
              - hbd:weekly
          daily:
            type: use_timing_daily
          weekly:
            type: use_timing_weekly
      datacenter:
        children_tags:
          tags:
            - hbd:useDetails
            - hbd:useTiming
        useDetails:
          tag: 
            tag: hbd:useDetails
          children_tags:
            tags:
              - espm:totalGrossFloorArea
              - espm:estimatesApplied
              - espm:coolingEquipmentRedundancy
              - espm:itEnergyMeterConfiguration
              - espm:upsSystemRedundancy
          totalGrossFloorArea:
            type: floor_area
          estimatesApplied:
            type: property_text
          coolingEquipmentRedundancy:
            type: property_text
          itEnergyMeterConfiguration:
            type: property_text
          upsSystemRedundancy:
            type: property_text
        useTiming:
          tag: 
            tag: hbd:useTiming
          children_tags:
            tags:
              - hbd:daily
              - hbd:weekly
          daily:
            type: use_timing_daily
          weekly:
            type: use_timing_weekly
      k12School:
        children_tags:
          tags:
            - hbd:useDetails
            - hbd:useTiming
        useDetails:
          tag: 
            tag: hbd:useDetails
          children_tags:
            tags:
              - espm:totalGrossFloorArea
              - espm:openOnWeekends
              - espm:numberOfWalkInRefrigerationUnits
              - espm:percentCooled
              - espm:percentHeated
              - espm:numberOfComputers
              - espm:cookingFacilities
              - espm:isHighSchool
              - espm:monthsInUse
              - espm:schoolDistrict
              - espm:studentSeatingCapacity
              - espm:numberOfWorkers
              - espm:gymnasiumFloorArea
              - espm:grossFloorAreaUsedForFoodPreparation
          totalGrossFloorArea:
            type: floor_area
          openOnWeekends:
            type: property_text
          numberOfWalkInRefrigerationUnits:
            type: property_decimal
          percentCooled:
            type: property_text
          percentHeated:
            type: property_text
          numberOfComputers:
            type: property_text
          cookingFacilities:
            type: property_text
          isHighSchool:
            type: property_text
          monthsInUse:
            type: property_text
          schoolDistrict:
            type: property_text
          studentSeatingCapacity:
            type: property_decimal
          numberOfWorkers:
            type: property_decimal
          gymnasiumFloorArea:
            type: floor_area
          grossFloorAreaUsedForFoodPreparation:
            type: property_decimal
        useTiming:
          tag: 
            tag: hbd:useTiming
          children_tags:
            tags:
              - hbd:daily
              - hbd:weekly
          daily:
            type: use_timing_daily
          weekly:
            type: use_timing_weekly
    
    meter_property_association_list:
      tag: 
        tag: hbd:meterPropertyAssociationList
      children_tags:
        tags:
          - hbd:energyMeterAssociation
          - hbd:occupancyMeterAssociation
          - hbd:averageEmissionsMeterAssociation
          - hbd:marginalEmissionsMeterAssociation
          - hbd:ambientTemperatureMeterAssociation
          - hbd:ambientWindSpeedMeterAssociation
          - hbd:ambientWindDirectionMeterAssociation
          - hbd:ambientHumidityMeterAssociation
          - hbd:ambientCloudCoverMeterAssociation
          - hbd:ambientDewPointMeterAssociation

      energyMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      occupancyMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      averageEmissionsMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit      

      marginalEmissionsMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      ambientTemperatureMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      ambientWindSpeedMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      ambientWindDirectionMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      ambientHumidityMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      ambientCloudCoverMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit

      ambientDewPointMeterAssociation:
        tag: 
          tag: hbd:meterPropertyAssociationList
        children_tags:
          tags:
            - hbd:meterURI
            - hbd:weight
            - hbd:externalWeight
            - espm:propertyRepresentation
            - hbd:tags
            - hbd:audit
        meterURI:
          type: text
        weight:
          type: decimal
        externalWeight:
          type: decimal
        propertyRepresentation:
          type: property_repr
        tags: 
          type: tags
        audit:
          type: audit  

    additional_info:
      children_tags:
        tags:
          - hbd:tags
          - hbd:audit
      tags:
        type: tags
      audit:
        type: audit
"""


@dataclass
class Tag:
    """Tag configuration structure"""

    tag: str


@dataclass
class Tags:
    """Tags configuration structure"""

    tags: list


@dataclass
class Type:
    """Type configuration structure"""

    type: str


@dataclass
class GeneralInfo:  # pylint: disable=too-many-instance-attributes
    """General Info configuration structure"""

    children_tags: "Tags"
    name: "Type"
    primaryFunction: "Type"  # pylint: disable=invalid-name
    grossFloorArea: "Type"  # pylint: disable=invalid-name
    yearBuilt: "Type"  # pylint: disable=invalid-name
    address: "Type"
    numberOfBuildings: "Type"  # pylint: disable=invalid-name
    isFederalProperty: "Type"  # pylint: disable=invalid-name
    occupancyPercentage: "Type"  # pylint: disable=invalid-name
    propertyURI: "Type"  # pylint: disable=invalid-name
    datacenterOver75kW: "Type"  # pylint: disable=invalid-name
    audit: "Type"


@dataclass
class UseTiming:
    """UseTImeng configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    daily: "Type"
    weekly: "Type"


@dataclass
class OfficeUseDetails:  # pylint: disable=too-many-instance-attributes
    """Office use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    totalGrossFloorArea: "Type"  # pylint: disable=invalid-name
    weeklyOperatingHours: "Type"  # pylint: disable=invalid-name
    numberOfWorkers: "Type"  # pylint: disable=invalid-name
    numberOfComputers: "Type"  # pylint: disable=invalid-name
    percentOfficeCooled: "Type"  # pylint: disable=invalid-name
    percentOfficeHeated: "Type"  # pylint: disable=invalid-name


@dataclass
class OfficeUse:
    """Office use configuration structure"""

    children_tags: "Tags"
    useDetails: "OfficeUseDetails"  # pylint: disable=invalid-name
    useTiming: "UseTiming"  # pylint: disable=invalid-name


@dataclass
class ParkingUseDetails:
    """Parking use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    supplementalHeating: "Type"  # pylint: disable=invalid-name
    openFootage: "Type"  # pylint: disable=invalid-name
    completelyEnclosedFootage: "Type"  # pylint: disable=invalid-name
    partiallyEnclosedFootage: "Type"  # pylint: disable=invalid-name


@dataclass
class ParkingUse:
    """Parking use configuration structure"""

    children_tags: "Tags"
    useDetails: "ParkingUseDetails"  # pylint: disable=invalid-name
    useTiming: "UseTiming"  # pylint: disable=invalid-name


@dataclass
class SupermarketUseDetails:  # pylint: disable=too-many-instance-attributes
    """Supermarket use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    totalGrossFloorArea: "Type"  # pylint: disable=invalid-name
    weeklyOperatingHours: "Type"  # pylint: disable=invalid-name
    numberOfWorkers: "Type"  # pylint: disable=invalid-name
    numberOfComputers: "Type"  # pylint: disable=invalid-name
    numberOfCashRegisters: "Type"  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    numberOfOpenClosedRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    percentCooled: "Type"  # pylint: disable=invalid-name
    percentHeated: "Type"  # pylint: disable=invalid-name
    singleStore: "Type"  # pylint: disable=invalid-name
    exteriorEntranceToThePublic: "Type"  # pylint: disable=invalid-name
    areaOfAllWalkInRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    lengthOfAllOpenClosedRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    cookingFacilities: "Type"  # pylint: disable=invalid-name


@dataclass
class SupermarketUse:
    """Supermarket use configuration structure"""

    children_tags: "Tags"
    useDetails: "SupermarketUseDetails"  # pylint: disable=invalid-name
    useTiming: "UseTiming"  # pylint: disable=invalid-name


@dataclass
class RetailUseDetails:  # pylint: disable=too-many-instance-attributes
    """Retail use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    totalGrossFloorArea: "Type"  # pylint: disable=invalid-name
    weeklyOperatingHours: "Type"  # pylint: disable=invalid-name
    numberOfWorkers: "Type"  # pylint: disable=invalid-name
    numberOfComputers: "Type"  # pylint: disable=invalid-name
    numberOfCashRegisters: "Type"  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    numberOfOpenClosedRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    percentCooled: "Type"  # pylint: disable=invalid-name
    percentHeated: "Type"  # pylint: disable=invalid-name
    singleStore: "Type"  # pylint: disable=invalid-name
    exteriorEntranceToThePublic: "Type"  # pylint: disable=invalid-name
    areaOfAllWalkInRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    lengthOfAllOpenClosedRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    cookingFacilities: "Type"  # pylint: disable=invalid-name


@dataclass
class RetailUse:
    """Retail use details configuration structure"""

    children_tags: "Tags"
    useDetails: "RetailUseDetails"  # pylint: disable=invalid-name
    useTiming: "UseTiming"  # pylint: disable=invalid-name


@dataclass
class DatacenterUseDetails:
    """Datacentr use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"  # pylint: disable=invalid-name
    totalGrossFloorArea: "Type"  # pylint: disable=invalid-name
    estimatesApplied: "Type"  # pylint: disable=invalid-name
    coolingEquipmentRedundancy: "Type"  # pylint: disable=invalid-name
    itEnergyMeterConfiguration: "Type"  # pylint: disable=invalid-name
    upsSystemRedundancy: "Type"  # pylint: disable=invalid-name


@dataclass
class DatacenterUse:
    """Datacentr use configuration structure"""

    children_tags: "Tags"
    useDetails: "DatacenterUseDetails"  # pylint: disable=invalid-name
    useTiming: "UseTiming"  # pylint: disable=invalid-name


@dataclass
class k12SchoolDetails:  # pylint: disable=too-many-instance-attributes,invalid-name
    """k12SchoolDetails use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    totalGrossFloorArea: "Type"  # pylint: disable=invalid-name
    openOnWeekends: "Type"  # pylint: disable=invalid-name
    numberOfWalkInRefrigerationUnits: "Type"  # pylint: disable=invalid-name
    percentCooled: "Type"  # pylint: disable=invalid-name
    percentHeated: "Type"  # pylint: disable=invalid-name
    numberOfComputers: "Type"  # pylint: disable=invalid-name
    cookingFacilities: "Type"  # pylint: disable=invalid-name
    isHighSchool: "Type"  # pylint: disable=invalid-name
    monthsInUse: "Type"  # pylint: disable=invalid-name
    schoolDistrict: "Type"  # pylint: disable=invalid-name
    studentSeatingCapacity: "Type"  # pylint: disable=invalid-name
    numberOfWorkers: "Type"  # pylint: disable=invalid-name
    gymnasiumFloorArea: "Type"  # pylint: disable=invalid-name
    grossFloorAreaUsedForFoodPreparation: "Type"  # pylint: disable=invalid-name


@dataclass
class k12SchoolUse:  # pylint: disable=invalid-name
    """k12SchoolUse use details configuration structure"""

    children_tags: "Tags"
    useDetails: "k12SchoolDetails"  # pylint: disable=invalid-name
    useTiming: "UseTiming"  # pylint: disable=invalid-name


@dataclass
class PropertyUses:  # pylint: disable=too-many-instance-attributes
    """Property use details configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    office: "OfficeUse"
    supermarket: "SupermarketUse"
    parking: "ParkingUse"
    retail: "RetailUse"
    datacenter: "DatacenterUse"
    k12School: "k12SchoolUse"  # pylint: disable=invalid-name


@dataclass
class EnergyMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """EnergyMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class OccupancyMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """OccupancyMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AverageEmissionsMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """OccupancyMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class MarginalEmissionsMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """MarginalEmissionsMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AmbientTemperatureMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """AmbientTemperatureMeterAssociationconfiguration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AmbientWindSpeedMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """AmbientWindSpeedMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AmbientWindDirectionMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """AmbientWindDirectionMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AmbientHumidityMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """AmbientHumidityMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AmbientCloudCoverMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """AmbientCloudCoverMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class AmbientDewPointMeterAssociation:  # pylint: disable=too-many-instance-attributes
    """AmbientDewPointMeterAssociation configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    meterURI: "Type"  # pylint: disable=invalid-name
    weight: "Type"
    externalWeight: "Type"  # pylint: disable=invalid-name
    propertyRepresentation: "Type"  # pylint: disable=invalid-name
    tags: "Type"
    audit: "Type"


@dataclass
class MeterPropertyAssociationList:  # pylint: disable=too-many-instance-attributes
    """MeterPropertyAssociationList configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    energyMeterAssociation: "EnergyMeterAssociation"  # pylint: disable=invalid-name
    occupancyMeterAssociation: "OccupancyMeterAssociation"  # pylint: disable=invalid-name
    averageEmissionsMeterAssociation: "AverageEmissionsMeterAssociation"  # pylint: disable=invalid-name
    marginalEmissionsMeterAssociation: "MarginalEmissionsMeterAssociation"  # pylint: disable=invalid-name
    ambientTemperatureMeterAssociation: "AmbientTemperatureMeterAssociation"  # pylint: disable=invalid-name
    ambientWindSpeedMeterAssociation: "AmbientWindSpeedMeterAssociation"  # pylint: disable=invalid-name
    ambientWindDirectionMeterAssociation: "AmbientWindDirectionMeterAssociation"  # pylint: disable=invalid-name
    ambientHumidityMeterAssociation: "AmbientHumidityMeterAssociation"  # pylint: disable=invalid-name
    ambientCloudCoverMeterAssociation: "AmbientCloudCoverMeterAssociation"  # pylint: disable=invalid-name
    ambientDewPointMeterAssociation: "AmbientDewPointMeterAssociation"  # pylint: disable=invalid-name


@dataclass
class AdditionalInfo:
    """AdditionalInfo configuration structure"""

    children_tags: "Tags"
    tags: "Type"
    audit: "Type"


@dataclass
class Property:
    """Property configuration structure"""

    tag: "Tag"
    children_tags: "Tags"
    general_info: "GeneralInfo"
    property_uses: "PropertyUses"
    meter_property_association_list: "MeterPropertyAssociationList"
    additional_info: "AdditionalInfo"


@dataclass
class PropertySchema:
    """Property schema structure"""

    property: "Property"


FACTORY = Factory()

PROPERTY_SCHEMA = FACTORY.load(
    yaml.safe_load(RAW_SCHEMA), PropertySchema
)


if __name__ == "__main__":
    import pprint

    def main():
        """Debug entrypoint"""
        pretty_print = pprint.PrettyPrinter(indent=4)
        factory = Factory()
        yaml_cfg = yaml.safe_load(RAW_SCHEMA)

        factory.load(yaml_cfg, PropertySchema)

        pretty_print.pprint(yaml_cfg)

    main()
