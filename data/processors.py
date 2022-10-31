
def test_demand_kw_in_bills(API_AUTH_TOKEN):
    no_demand_kw = []
    all_active = get_active_meters(API_AUTH_TOKEN)
    for i in all_active:
        try:
            get_bills(API_AUTH_TOKEN, i)["Demand_kw"]
            return_code = 0
        except Exception as e:
            return_code = 1
        if return_code == 1:
            no_demand_kw.append(i)
    return no_demand_kw