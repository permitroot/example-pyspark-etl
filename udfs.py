import re

def get_english_name(species):
    return re.sub('\s\([^)]*\)', repl='', string=species)

def get_start_year(period):
    return period[1:-1].split("-")[0]

def get_trend(annual_percentage_change):
    trend = ''
    if annual_percentage_change < -3.00 :
        trend = 'stong decline'
    elif annual_percentage_change >= -3.00 and annual_percentage_change <= -0.50 :
        trend = 'weak decline'
    elif annual_percentage_change > -0.50 and annual_percentage_change < -0.50 :
        trend = 'no change'
    elif annual_percentage_change >= 0.50 and annual_percentage_change <= 3.00 :
        trend = 'weak increase'
    else :
        trend = 'strong increase'
    return trend