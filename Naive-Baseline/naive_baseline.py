import requests
import json
import pandas as pd
def naivetopk(k, data, scoring_criteria):
    topk = []
    for pivot in data:
        if topk == []:
            topk.append(pivot)
        else:
            filled = False
            end = False
            i = len(topk) - 1
            while i >= 0 and end == False:
                LLM_call = "For l=" + str(pivot) + " and r=" + str(topk[i]) + " given following scoring criteria: [" + scoring_criteria + "] which scores higher (return only the letter l or r)"
                print(LLM_call)
                response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": "Bearer " + json.load(open("key.json", "r"))["key"],
                    "Content-Type": "application/json",
                },
                data=json.dumps({
                    "model": "nvidia/nemotron-nano-9b-v2:free",
                    "messages": [
                    {
                        "role": "user",
                        "content": LLM_call
                    }
                    ],
                   
                })
                )
                print(response.json())
                LLM_result = response.json()['choices'][0]['message']['content'].strip()
                print(LLM_result)
                if LLM_result == "r":
                    filled = True
                    if i == len(topk)-1:
                        topk.append(pivot)
                    else:
                        topk.insert(i + 1, pivot)
                    end = True
                i = i - 1
            if filled == False:
                topk.insert(0, pivot)
            if len(topk) > k:
                topk = topk[:k]
    return topk

def read_json(json_file):
    data = json.load(open(json_file, "r"))
    return data["K"], data["Criteria"], pd.read_parquet(data["Filename"], engine="pyarrow")

def organize_pandas_rowwise(data):
    items = []
    for i in range(len(data)):
        row_items = data.iloc[i]
        string_version = "{"
        for r in row_items:
            string_version = string_version + str(r) + ","
        string_version = string_version[:-1] + "}"
        items.append(string_version)
    return items


test_data = pd.DataFrame([["like", "as", "when"], ["divert", "conflict", "organize"], ["divergence", "coagulate", "contextualize"]])
test_data.to_parquet("test_data.parquet", engine="pyarrow", index=False)
params = read_json("input.json")
print(naivetopk(params[0], organize_pandas_rowwise(params[2]), params[1]))