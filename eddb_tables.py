#!/usr/bin/env python


import sys
import re


import requests
import bs4


def _tables_from_html(html, id_=None):
    soup = bs4.BeautifulSoup(html, features="html.parser")
    if id_:
        return soup.find(attrs={"id": id_})
    else:
        return soup.findAll('table')


def _tonumeric(s):
    if s.strip():
        return int(s.replace(",", "").replace("%", "").strip())
    else:
        return None


def _toelapsed(s):
    m = re.search("(\d+)\s+(\S+)$", s)
    if m:
        (num, unit) = (m.group(1), m.group(2))
        if unit == "days":
            mult = 3600*24
        elif unit == "hours":
            mult = 3600
        elif unit == "mins":
            mult = 60
        elif unit == "secs":
            mult = 1
        else:
            raise ValueError(f"Unknown unit: {unit}")
        return _tonumeric(num)*mult
    else:
        return None


def _parse_table(table, headers, transform=None):
    transform = transform or {}
    data = [
        [el.text.strip() for el in row.find_all('td')]
        for row in table.findAll('tr')
    ]
    result = []
    for row in data:
        try:
            if row:
                result.append(dict(zip(headers, row)))
        except IndexError:
            print("Could not parse row: '{row}'", file=sys.stderr)
    for entry in result:
        for (field, xform) in transform.items():
            try:
                entry[field] = xform(entry[field])
            except KeyError:
                print(entry)
                raise
    return result


def commodity_mapping():
    url = "https://eddb.io/commodity"
    table_id = "commodities-table"
    headers = ["Type", "Name", "Buy", "Price", "Sell", "Profit", "Buy%", "Sell%"]
    transforms = {
        field: _tonumeric
        for field in ["Buy", "Price", "Sell", "Profit", "Buy%", "Sell%"]
    }

    r = requests.get(url)
    r.raise_for_status()
    table = _tables_from_html(r.text, id_=table_id)
    return _parse_table(table, headers, transforms)


def commodity_buy(id_):
    url = f"https://eddb.io/commodity/{id_}"
    table_id = "table-stations-min-buy"
    headers = ["Station", "System", "Price", "Compare", "Supply", "Pad", "Time"]
    transforms = {
        field: _tonumeric
        for field in ["Price", "Supply"]
    }
    transforms["Time"] = _toelapsed

    r = requests.get(url)
    r.raise_for_status()
    table = _tables_from_html(r.text, id_=table_id)
    return _parse_table(table, headers, transforms)


def commodity_sell(id_):
    url = f"https://eddb.io/commodity/{id_}"
    table_id = "table-stations-max-sell"
    headers = ["Station", "System", "Price", "Compare", "Demand", "Pad", "Time"]
    transforms = {
        field: _tonumeric
        for field in ["Price", "Demand"]
    }
    transforms["Time"] = _toelapsed

    r = requests.get(url)
    r.raise_for_status()
    table = _tables_from_html(r.text, id_=table_id)
    return _parse_table(table, headers, transforms)
