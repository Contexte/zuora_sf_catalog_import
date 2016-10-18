# -*- coding: utf-8 -*-
"""
    Generate the product catalog objects from the price list
    and bulk create the objects in Salesforce
    Salesforce subscribers.
"""
import json
import csv
from copy import copy
import logging
import os
import sys
from time import sleep


from simple_salesforce import SalesforceLogin
from salesforce_bulk import SalesforceBulk
from pyjavaproperties import Properties

from utils import CsvDictsAdapter

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch_debug = logging.StreamHandler()
ch_debug.setLevel(logging.DEBUG)
ch_debug.setFormatter(debug_formatter)

logger.addHandler(ch_debug)

# CONFIGURATION

sf_properties = Properties()
sf_properties.load(open('../salesforce.properties'))

product_tpl = {
    "FamilyZuora__c": "Abonnement",
    "zqu__Allow_Feature_Changes__c": "false",
    "zqu__Category__c": "Base Products",
    "zqu__EffectiveEndDate__c": "2021-12-31",
    "zqu__EffectiveStartDate__c": "2016-09-01",
    "zqu__Type__c": "Standalone"
}

rate_plan_tpl = {
    "SubscriptionPlanType__c": "payant",
    "zqu__ActiveCurrencies__c": "EUR",
    "zqu__EffectiveEndDate__c": "2021-12-31",
    "zqu__EffectiveStartDate__c": "2016-09-01",
}


rate_plan_charge_tpl = {
    "zqu__AccountingCode2__c": u"Produits constat\u00e9s d'avance Abonnement",
    "zqu__BillCycleType__c": "Charge Trigger Day",
    "zqu__BillingPeriodAlignment__c": "Align To Charge",
    "zqu__EndDateCondition__c": "Subscription End Date",
    "zqu__ListPriceBase__c": "Per Billing Period",
    "zqu__DeferredRevenueAccount__c": u"Produits constat\u00e9s d'avance Abonnement",
    "zqu__Model__c": "Flat Fee Pricing",
    "zqu__RecognizedRevenueAccount__c": "Prestation de services Abonnement",
    # "zqu__RevRecCode2__c": "Default",
    # "zqu__RevRecTriggerCondition__c": "Contract Effective Date",
    "zqu__RevenueRecognitionRuleName__c": "Recognize daily over time",
    "zqu__TaxCode2__c": "Presse",
    "zqu__Taxable__c": "true",
    "zqu__TaxMode__c": "Tax Exclusive",
    "zqu__TriggerEvent__c": "Upon Contract Effective",
    "zqu__Type__c": "Recurring"
}


WRITE = True


# MAKING OBJECTS

products = [
    {
        'Name': u'Abonnement pouvoirs',
        "NameEN__c": "Power subscription",
        "ProductCode": "fr_fr_pro_content-spe_power",
        "ImportExternalId__c": "fr_fr_pro_content-spe_power"

    },
    {
        "Name": u"Abonnement \u00e9nergie",
        "NameEN__c": "Energy subscription",
        "ProductCode": "fr_fr_pro_content-spe_energy",
        "ImportExternalId__c": "fr_fr_pro_content-spe_energy",
    },
    {
        "Name": u"Abonnement transport",
        "NameEN__c": "Transportation subscription",
        "ProductCode": "fr_fr_pro_content-spe_transportation",
        "ImportExternalId__c": "fr_fr_pro_content-spe_transportation",
    },
    {
        "Name": u"Abonnement num\u00e9rique",
        "NameEN__c": "Digital subscription",
        "ProductCode": "fr_fr_pro_content-spe_digital",
        "ImportExternalId__c": "fr_fr_pro_content-spe_digital",
    },
]

products = [dict(product.items() + product_tpl.items()) for product in products]


rate_plans = []
rate_plan_charges = []
rate_plan_charge_tiers = []

for product in products:
    product_rate_plans_yearly = []
    product_rate_plans_monthly = []
    product_rate_plan_charges_yearly = []
    product_rate_plan_charges_monthly = []
    product_rate_plan_charge_tiers_yearly = []
    product_rate_plan_charge_tiers_monthly = []
    with open('../catalogue.csv') as catalog:
        reader = csv.reader(catalog, delimiter=",")
        for code, org_type, \
            employees_min, employees_max, \
            reader_min, reader_max, \
            price_year, price_month in reader:

            ############
            # Rate plans
            ############
            rate_plan = copy(rate_plan_tpl)
            rate_plan['OrganizationType__c'] = org_type
            rate_plan['NumberOfEmployeesMax__c'] = employees_max
            rate_plan['NumberOfEmployeesMin__c'] = employees_min
            rate_plan['PotentialReadersMax__c'] = reader_max
            rate_plan['PotentialReadersMin__c'] = reader_min
            rate_plan['SubscriptionPlanType__c'] = 'payant'
            rate_plan['zqu__ActiveCurrencies__c'] = "EUR"
            rate_plan['zqu__EffectiveStartDate__c'] = "2016-09-01"
            rate_plan['zqu__EffectiveEndDate__c'] = "2021-12-31"
            rate_plan['zqu__Product__r'] = {
                'ImportExternalId__c': product['ImportExternalId__c']
            }

            rate_plan_yearly = copy(rate_plan)
            rate_plan_yearly["ImportExternalId__c"] = '{}-{}-y'.format(
                product['ProductCode'],
                code.lower()
            )

            rate_plan_yearly['Name'] = "{} Annuel".format(code)
            rate_plan_yearly['zqu__ProductRatePlanFullName__c'] = rate_plan_yearly['Name']

            product_rate_plans_yearly.append(rate_plan_yearly)

            rate_plan_monthly = copy(rate_plan)
            rate_plan_monthly["ImportExternalId__c"] = '{}-{}-m'.format(
                product['ProductCode'],
                code.lower()
            )
            rate_plan_monthly['Name'] = "{} Mensuel".format(code)
            rate_plan_monthly['zqu__ProductRatePlanFullName__c'] = rate_plan_monthly['Name']

            product_rate_plans_monthly.append(rate_plan_monthly)

            ###################
            # Rate plan charges
            ###################
            rate_plan_charge_yearly = copy(rate_plan_charge_tpl)
            rate_plan_charge_yearly['Name'] =  'Licence annuelle'
            rate_plan_charge_yearly['zqu__ProductRatePlanChargeFullName__c'] =  'Licence annuelle'
            rate_plan_charge_yearly['zqu__RecurringPeriod__c'] = 'Annual'
            rate_plan_charge_yearly['zqu__ProductRatePlan__r'] = {
                'ImportExternalId__c': rate_plan_yearly['ImportExternalId__c']
            }
            rate_plan_charge_yearly["ImportExternalId__c"] = rate_plan_yearly["ImportExternalId__c"]
            product_rate_plan_charges_yearly.append(rate_plan_charge_yearly)

            rate_plan_charge_monthly = copy(rate_plan_charge_tpl)
            rate_plan_charge_monthly['Name'] =  'Licence mensuelle'
            rate_plan_charge_monthly['zqu__ProductRatePlanChargeFullName__c'] =  'Licence mensuelle'
            rate_plan_charge_monthly['zqu__RecurringPeriod__c'] = 'Month'
            rate_plan_charge_monthly['zqu__ProductRatePlan__r'] = {
                'ImportExternalId__c': rate_plan_monthly['ImportExternalId__c']
            }
            rate_plan_charge_monthly["ImportExternalId__c"] = rate_plan_monthly["ImportExternalId__c"]
            product_rate_plan_charges_monthly.append(rate_plan_charge_monthly)


            ########################
            # Rate plan charge tiers
            ########################
            rate_plan_charge_tier_yearly = {
                'Name': 1,
                'zqu__Price__c': price_year,
                'zqu__Tier__c': 1,
                'zqu__StartingUnit__c': 0,
                'zqu__PriceFormat__c': 'Flat Fee',
                'zqu__Currency2__c': 'EUR',
                'zqu__ProductRatePlanCharge__r': {
                    'ImportExternalId__c': rate_plan_charge_yearly['ImportExternalId__c']
                }
            }
            product_rate_plan_charge_tiers_yearly.append(rate_plan_charge_tier_yearly)

            rate_plan_charge_tier_monthly = {
                'Name': 1,
                'zqu__Price__c': price_month,
                'zqu__Tier__c': 1,
                'zqu__StartingUnit__c': 0,
                'zqu__Currency2__c': 'EUR',
                'zqu__PriceFormat__c': 'Flat Fee',
                'zqu__ProductRatePlanCharge__r': {
                    'ImportExternalId__c': rate_plan_charge_monthly['ImportExternalId__c']
                }
            }
            product_rate_plan_charge_tiers_monthly.append(rate_plan_charge_tier_monthly)

    rate_plans.append(product_rate_plans_yearly)
    rate_plans.append(product_rate_plans_monthly)

    rate_plan_charges.append(product_rate_plan_charges_yearly)
    rate_plan_charges.append(product_rate_plan_charges_monthly)

    rate_plan_charge_tiers.append(product_rate_plan_charge_tiers_yearly)
    rate_plan_charge_tiers.append(product_rate_plan_charge_tiers_monthly)



# Salesforce session
#

session_id, instance = SalesforceLogin(
    username=sf_properties['sf.username'],
    password=sf_properties['sf.password'][:-len(sf_properties['sf.token'])],
    security_token=sf_properties['sf.token'],
    sandbox=False
)


bulk = SalesforceBulk(sessionId=session_id, host=instance, API_version="36.0")

if WRITE:
    # bulk import products

    job = bulk.create_insert_job("Product2", contentType='JSON')

    batch = bulk.post_bulk_batch(job, json.dumps(products), contentType='application/json')

    bulk.close_job(job)

    logger.info("Products bulk created!")
    sleep(10)

    # bulk import rate plans
    job = bulk.create_insert_job("zqu__ProductRatePlan__c", contentType='JSON')
    for rate_plan in rate_plans:
        batch = bulk.post_bulk_batch(job, json.dumps(rate_plan), contentType='application/json')
    bulk.close_job(job)

    logger.info("RatePlans bulk created!")
    sleep(10)

    # bulk import rate plan charges
    job = bulk.create_insert_job("zqu__ProductRatePlanCharge__c", contentType='JSON')
    for rate_plan_charge in rate_plan_charges:
        batch = bulk.post_bulk_batch(job, json.dumps(rate_plan_charge), contentType='application/json')
    bulk.close_job(job)

    logger.info("RatePlanCharges bulk created!")
    sleep(10)

    # bulk import rate plan charge tiers
    job = bulk.create_insert_job("zqu__ProductRatePlanChargeTier__c", contentType='JSON')
    for rate_plan_charge_tier in rate_plan_charge_tiers:
        batch = bulk.post_bulk_batch(job, json.dumps(rate_plan_charge_tier), contentType='application/json')
    bulk.close_job(job)

    logger.info("RatePlanChargeTiers bulk created!")
    sleep(10)


else:

    job = bulk.create_query_job('zqu__ProductRatePlanCharge__c', contentType='CSV')

    fields = [
        'Name',
        'zqu__ProductRatePlanChargeFullName__c',
        'zqu__Type__c',
        'zqu__ProductRatePlan__c',
        'zqu__Model__c',
        'zqu__ListPriceBase__c',
        'zqu__TriggerEvent__c',
        'zqu__BillCycleDay__c',
        'zqu__RecurringPeriod__c',
        'zqu__BillCycleType__c',
        'zqu__BillingPeriodAlignment__c',
        'zqu__EndDateCondition__c',
        'zqu__RevenueRecognitionRuleName__c',
        'zqu__DeferredRevenueAccount__c',
        'zqu__RecognizedRevenueAccount__c',
        'zqu__RevRecTriggerCondition__c',
        'zqu__AccountingCode2__c',
        'zqu__RevRecCode2__c',
        'zqu__Taxable__c',
        'zqu__TaxCode2__c',
        'ImportExternalId__c',
    ]
    query = \
        u"Select {fields} from zqu__ProductRatePlanCharge__c \
        Where Id = 'a1e3B000000DzpcQAC'".format(fields=', '.join(fields))

    # fields = [
    #     'Name',
    #     'zqu__ProductRatePlanFullName__c',
    #     'zqu__Product__c',
    #     'NumberOfEmployeesMin__c',
    #     'NumberOfEmployeesMax__c',
    #     'PotentialReadersMin__c',
    #     'PotentialReadersMax__c',
    #     'zqu__EffectiveStartDate__c',
    #     'zqu__EffectiveEndDate__c',
    #     'zqu__ActiveCurrencies__c',
    #     'SubscriptionPlanType__c',
    #     'ImportExternalId__c'
    # ]
    # query = \
    #     u"Select {fields} from zqu__ProductRatePlan__c \
    #     Where Id = 'a1f3B000000EvOh'".format(fields=', '.join(fields))


    batch = bulk.query(job, query.encode('utf-8'))
    while not bulk.is_batch_done(job, batch):
        sleep(10)
    bulk.close_job(job)

    for row in bulk.get_batch_result_iter(job, batch, parse_csv=True):
        print json.dumps(row, sort_keys=True,
                        indent=4, separators=(',', ': '))
