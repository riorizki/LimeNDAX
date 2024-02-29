import warnings
import mmap
import numpy as np
import pandas as pd
import os
import zipfile
from sys import displayhook
from datetime import timedelta
from datetime import datetime
import re
from . import ndax_basic
import codecs
import xml.etree.ElementTree as ET
import struct
import logging


def valid_rec(bytes):
    # identify a valid record
    [Status] = struct.unpack("<B", bytes[17:18])
    return (Status != 0) & (Status != 255)


def fabricate(df):
    """
    Some ndax from from BTS Server 8 do not seem to contain a complete dataset.
    This helper function fills in missing times, capacities, and energies.
    """
    # Identify the valid data
    nan_value = df["Time"].notnull()

    if nan_value.any():
        warnings.warn(
            "IMPORTANT:This ndax is of the newest version.\n\
            The output from Code contains calculated data for time, timeinstep, capacity and energy, same as how NEWARE does."
        )
    # get the general time record condition for whole dataframe
    trc = df["Time"].diff()
    trd = trc.mode().values[0]
    list1 = df[df["Time"] == 0.0].index.values
    list0 = df[df["Time"] == trd].index.values - 1
    # list2=df[df['Time']==0.0].index.values+1
    s1 = np.setdiff1d(list0, list1)
    s2 = np.setdiff1d(list1, list0) + 1
    for i in s1:
        df.loc[i, "Cycle"] = df.loc[i + 1, "Cycle"]
        df.loc[i, "Step"] = df.loc[i + 1, "Step"]
        df.loc[i, "Status"] = df.loc[i + 1, "Status"]
        df.loc[i, "Cycle"] = df.loc[i + 1, "Cycle"]

    df["Time"] = [0.0 if idx in s1 else val for idx, val in enumerate(df["Time"])]
    df["Time"] = [trd if idx in s2 else val for idx, val in enumerate(df["Time"])]

    df["Time"] = df.groupby("Step")["Time"].transform(
        lambda x: pd.Series.interpolate(x, limit_area="inside")
    )
    # Perform extrapolation to generate the remaining missing Time
    nan_value2 = df["Time"].notnull()
    time_inc = df["Time"].diff().ffill().groupby(nan_value2.cumsum()).cumsum()
    time = df["Time"].ffill() + time_inc.shift()
    df["Time"].where(nan_value2, time, inplace=True)

    # Fill in missing Timestamps
    time_inc = df["Time"].diff().groupby(nan_value.cumsum()).cumsum()
    timestamp = df["Timestamp"].ffill() + pd.to_timedelta(time_inc.shift(), unit="S")
    df["Timestamp"].where(nan_value, timestamp, inplace=True)

    # capacity calculation
    df["Capacity(Ah)"].where(df["Time"] != 0.0, 0.0, inplace=True)
    cap_mask = df["Capacity(Ah)"].notnull()
    capacity = (
        (df["Time"].diff().where(df["Time"].diff() >= 0, df["Time"].diff().shift(1)))
        * abs(df["Current(A)"])
        / 3600
    )
    inc = capacity.groupby(cap_mask.cumsum()).cumsum()
    cap = df["Capacity(Ah)"].ffill() + inc.where(df["Current(A)"] != 0, 0).shift()
    df["Capacity(Ah)"].where(nan_value, cap, inplace=True)

    # energy calculation
    energy = capacity * df["Voltage"]
    inc = energy.groupby(nan_value.cumsum()).cumsum()
    eng = df["Energy(Wh)"].ffill() + inc.where(df["Current(A)"] != 0, 0).shift()
    df["Energy(Wh)"].where(nan_value, eng, inplace=True)

    df["Cycle"] = df["Cycle"].interpolate()
    df["Status"] = df["Status"].ffill()


def data_ndc(file):
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # Identify the header and record lenght
        record_len = 4096
        header = 4096

        # Read data
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack("<ff", bytes[132:-4]):
                if i[0] != 0:
                    rec.append([i[0] / 10000, i[1] / 1000])

    # Create DataFrame
    df = pd.DataFrame(rec, columns=["Voltage", "Current(A)"])
    df["Index"] = df.index + 1
    return df


def data_runInfo_ndc(file):
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # ndc file versions has different bytes possition
        [ndc_version] = struct.unpack("<B", mm[2:3])
        # for ndc version 11
        format = "<isffff12siii2s"
        end_byte = -63
        if ndc_version >= 14:
            format = "<isffff12siii10s"
            end_byte = -59

        record_len = 4096
        header = 4096

        # Read data
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack(format, bytes[132:end_byte]):
                Time = i[0]
                [Charge_Capacity, Discharge_Capacity] = [i[2], i[3]]
                [Charge_Energy, Discharge_Energy] = [i[4], i[5]]
                [Timestamp, Step, Index] = [i[7], i[8], i[9]]
                if Index != 0:
                    rec.append(
                        [
                            Time / 1000,
                            abs(
                                (Charge_Capacity / 3600000)
                                - (Discharge_Capacity / 3600000)
                            ),
                            abs(
                                (Charge_Energy / 3600000) - (Discharge_Energy / 3600000)
                            ),
                            datetime.fromtimestamp(Timestamp),
                            Step,
                            Index,
                        ]
                    )

    # Create DataFrame
    df = pd.DataFrame(
        rec,
        columns=["Time", "Capacity(Ah)", "Energy(Wh)", "Timestamp", "Step", "Index"],
    )
    df["Timestamp"] = (
        df["Timestamp"].dt.tz_localize("Asia/Dhaka").dt.tz_convert("Asia/Kolkata")
    ).dt.tz_localize(None)
    df["Timestamp"] = df["Timestamp"].dt.round("1s")
    df["Step"] = ndax_basic.count_changes(df["Step"])

    return df


def data_step_ndc(file):
    with open(file, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        mm_size = mm.size()

        # Identify record length and header
        record_len = 4096
        header = 4096

        # Read data
        rec = []
        mm.seek(header)
        while mm.tell() < mm_size:
            bytes = mm.read(record_len)
            for i in struct.iter_unpack("<ii16sb12s", bytes[132:-5]):
                [Cycle, Step_Index, Status] = [i[0], i[1], i[3]]
                if Step_Index != 0:
                    rec.append([Cycle + 1, Step_Index, ndax_basic.state_dict[Status]])

    # Create DataFrame
    df = pd.DataFrame(rec, columns=["Cycle", "Step", "Status"])
    df["Step"] = df.index + 1
    return df


def to_df(file, include_aux: bool = False, step_cyclic_id: bool = False):
    """
    Internal Function do not use.

    Function to convert data.ndc binary data file generated from a Neware to Dataframe.

    Args:
        file (str): Name of an .ndax file to read

        include_aux (bool): if pass True it will add temperature data if there is any while testing

    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """
    # stime = time()
    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    f = zipfile.ZipFile(file, "r")
    for x in f.namelist():
        f.extract(x, "./temdata/")
    f.close()
    # files = os.listdir("./temdata/")
    data_file = "./temdata/data.ndc"
    # ctime = time()
    # print("Time to extract: ",ctime - stime )

    dtype_dict = {
        "Index": "uint32",
        "Cycle": "uint16",
        "Step": "uint32",
        "Status": "str",
        # 'Status': 'category',
        "Time": "float32",
        "Voltage": "float32",
        "Current(A)": "float32",
        "Capacity(Ah)": "float32",
        "Energy(Wh)": "float32",
        # 'Timestamp':'str',
        "Validated": "bool",
        # 'DCIR(mOhm)': 'float32',
    }

    # Ndax generated from server version 8 has data spread across 3 different ndc files.
    # Version <8 have all data in data.ndc.
    # for version 8 files check if data_runInfo.ndc and data_step.ndc exist while data.ndc will be present in both.
    if all(
        i in os.listdir(r".\temdata") for i in ["data_runInfo.ndc", "data_step.ndc"]
    ):

        # Read data from separate files
        runInfo_file = "./temdata/data_runInfo.ndc"
        step_file = "./temdata/data_step.ndc"

        # i, v, c
        data_df = data_ndc(data_file)
        # tis, cap, eng, ts, stepid, index
        runInfo_df = data_runInfo_ndc(runInfo_file)
        # cycle, step,stepname
        step_df = data_step_ndc(step_file)

        # Merge dataframes
        data_df = data_df.merge(runInfo_df, how="left", on="Index")
        data_df["Step"].ffill(inplace=True)
        data_df = data_df.merge(step_df, how="left", on="Step").reindex(
            columns=ndax_basic.rec_columns
        )

        # fabricate data for ndax generated from Neware server 8.
        fabricate(data_df)
        data_df = data_df.astype(dtype=dtype_dict)
        # ndax_basic.validate_timegap(data_df) #added in validator_fab()
        data_df["Time"] = data_df["Time"].apply(lambda x: np.round(x, decimals=2))
        ndax_basic.validator_fab(data_df)
        return data_df

    else:
        with open(data_file, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

            # identify record length, and onset and set the header accordingly
            record_len = 94
            offset = 0
            onset = mm[517:525]
            aux_id = slice(0, 1)
            rec_byte = slice(0, 1)
            if onset == b"\x00\x00\x00\x00\x00\x00\x00\x00":
                record_len = 90
                offset = 4
                onset = mm[4225:4229]
                aux_id = slice(3, 4)
                rec_byte = slice(7, 8)
            output = []
            aux = []
            header = mm.find(onset)
            while header != -1:
                mm.seek(header - offset)
                bytes = mm.read(record_len)
                if bytes[rec_byte] == b"\x55":
                    if valid_rec(bytes):
                        output.append(ndax_basic.byte_to_list(bytes))
                else:
                    logging.warning("Unknown record type: " + bytes[rec_byte].hex())

                if include_aux:
                    if bytes[aux_id] == b"\x65":
                        aux.append(ndax_basic.aux_bytes65(bytes))

                    elif bytes[aux_id] == b"\x74":
                        aux.append(ndax_basic.aux_bytes74(bytes))

                header = mm.find(onset, header - offset + record_len)
        # ctime = time()
        # print("Time to get all bytes: ",ctime - stime )
        df = pd.DataFrame(output, columns=ndax_basic.rec_columns)
        df.dropna(inplace=True)
        df.drop_duplicates(subset="Index", inplace=True)

        if not df.Index.is_monotonic_increasing:
            df.sort_values("Index", inplace=True)

        df.reset_index(drop=True, inplace=True)
        # ctime = time()
        # print("Time for df: ",ctime - stime )
        ndax_basic.validate_timegap(df)
        # ctime = time()
        # print("Time for timegap: ",ctime - stime )
        # Join temperature data
        if include_aux:
            aux_df = pd.DataFrame(aux, columns=ndax_basic.aux_columns)
            aux_df.drop_duplicates(inplace=True)
            if not aux_df.empty:
                df = df.merge(aux_df, on=["Index"])

        if step_cyclic_id == True:
            df["Step Number"] = df["Step"]

        df.Step = ndax_basic.count_changes(df.Step)

        if "DCIR(mOhm)" not in df.keys():
            df["prev_cur"] = df["Current(A)"].shift(periods=1)
            df["prev_vol"] = df["Voltage"].shift(periods=1)
            df["DCIR(mOhm)"] = -1.0
            df.loc[((df["prev_cur"] == 0) & (df["Current(A)"] != 0)), "DCIR(mOhm)"] = (
                abs(
                    (df["Voltage"] - df["prev_vol"])
                    / (df["Current(A)"] - df["prev_cur"])
                )
                * 1000000
            ).astype("float32")
            df.drop(columns=["prev_cur", "prev_vol"], inplace=True)

        # ctime = time()
        # print("Time to extract: ",ctime - stime )
        df = df.astype(dtype=dtype_dict)
        return df


def get_records(
    file,
    rename: bool = False,
    drop_cycle_if_gap: bool = False,
    include_aux: bool = False,
    step_cyclic_id: bool = False,
):
    """
    Function to convert data.ndc binary data file generated from a Neware to Dataframe.

    Args:
        file (str): Name of an .ndax file to read

        drop_cycle_if_gap (bool): if True it will return modified data which will be same as BTS Client data.
            If there is gap in time stamp or Data Point or in Cycle. NDAx_decoder will read bytes as it is.
            Neware BTS Client will make modification even if there is data missing to maintain the continuity of Cycle ID.

        include_aux (bool): if pass True then it will add Temperature if it is there.

    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """

    rec_columns_x = {
        "Index": "DataPoint",
        "Cycle": "Cycle Index",
        "Step": "Step Index",
        "Status": "Step Type",
        "Time": "Time",
        "Voltage": "Voltage(V)",
        "Current(A)": "Current(A)",
        "Capacity(Ah)": "Capacity(Ah)",
        "Energy(Wh)": "Energy(Wh)",
        "Timestamp": "Timestamp",
        "Validated": "bool",
        "DCIR(mOhm)": "DCIR(mOhm)",
    }
    rec_columns1 = {
        "Index": "record_ID",
        "Cycle": "cycle",
        "Step": "step_ID",
        "Status": "step_name",
        "Time": "time_in_step",
        "Voltage": "voltage_V",
        "Current(A)": "current_mA",
        "Capacity(Ah)": "capacity_mAh",
        "Energy(Wh)": "energy_mWh",
        "Timestamp": "timestamp",
        "Validated": "Validated",
        "DCIR(mOhm)": "DCIR(mOhm)",
    }

    df = to_df(file, include_aux=include_aux, step_cyclic_id=step_cyclic_id)

    if "DCIR(mOhm)" not in df.keys():
        df["prev_cur"] = df["Current(A)"].shift(periods=1)
        df["prev_vol"] = df["Voltage"].shift(periods=1)
        df["DCIR(mOhm)"] = -1.0
        df.loc[((df["prev_cur"] == 0) & (df["Current(A)"] != 0)), "DCIR(mOhm)"] = (
            abs((df["Voltage"] - df["prev_vol"]) / (df["Current(A)"] - df["prev_cur"]))
            * 1000000
        ).astype("float32")
        df.drop(columns=["prev_cur", "prev_vol"], inplace=True)

    if drop_cycle_if_gap:
        """
        It will drop the cycle and and previous cycle where there is gap.

        """
        for i in df[df["Index"].diff() > 1].index.values:
            cyc = df.at[i, "Cycle"]
            prev_cyc = cyc - 1
            while prev_cyc not in df["Cycle"]:
                prev_cyc -= 1
            df.drop(df[df["Cycle"] == cyc].index, inplace=True)
            df.drop(df[df["Cycle"] == prev_cyc].index, inplace=True)
            df.reset_index(drop=True, inplace=True)

        # df['Index']=ndax_basic.count_changes(df['Index'])
        # temp = df.iloc[-1]['Index']
        # df.iloc[-1]['Index']=temp+1
        # df['Cycle']=ndax_basic.count_changes(df['Cycle'])
        # df['Step']=ndax_basic.count_changes(df['Step'])
        # df['Step Number'] = ndax_basic.count_changes(df['Step'])

    if rename:
        df = df.rename(columns=rec_columns_x)
    else:
        """
        data is in A and W after renaming change to mA and mW
        """
        df = df.rename(columns=rec_columns1)
        df["current_mA"] = df["current_mA"].mul(1000)
        df["capacity_mAh"] = df["capacity_mAh"].mul(1000)
        df["energy_mWh"] = df["energy_mWh"].mul(1000)

    return df


def get_stepxml(ndax_file):
    """
    Do Not Use. It is the recipe file generate after xml file which do not always have all the data.

    Args:
        file (str): Name of an .ndax file to read

    Returns:
        df (pd.DataFrame): DataFrame containing all records in the file
    """

    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    f = zipfile.ZipFile(ndax_file, "r")
    for x in f.namelist():
        f.extract(x, "./temdata/")
    f.close()
    files = os.listdir("./temdata/")
    tempfile = ""
    for file in files:
        if len(file.split("_")) > 2:
            tempfile = "./temdata/" + file
    file = "./temdata/Step.xml"

    with codecs.open(file, "r", "GB2312") as file:
        xml_content = file.read()

    root = ET.fromstring(xml_content)

    data_list = []

    for step in root.findall(".//Step_Info/*"):
        step_data = {
            "Step_ID": step.get("Step_ID"),
            "Step_Type": step.get("Step_Type"),
        }

        # Extract information from nested elements
        for sub_element in step.findall("*"):
            if sub_element.tag in ("Record", "Limit", "Protect", "AdvancedPrt"):
                for sub_sub_element in sub_element.findall("*"):
                    for sub_sub_sub_element in sub_sub_element.findall("*"):
                        value_key = f"{sub_element.tag}_{sub_sub_element.tag}_{sub_sub_sub_element.tag}"
                        value = sub_sub_sub_element.get("Value")
                        step_data[value_key] = value
                    for sub_sub_sub_sub_element in sub_sub_sub_element.findall("*"):
                        value_key = f"{sub_element.tag}_{sub_sub_element.tag}_{sub_sub_sub_element.tag}_{sub_sub_sub_sub_element.tag}"
                        value = sub_sub_sub_sub_element.get("Value")
                        step_data[value_key] = value
        data_list.append(step_data)
    df = pd.DataFrame(data_list)
    print(df.columns)
    df_1 = df.fillna(0)
    df_1["Record_Main_Time"] = df_1["Record_Main_Time"].astype("int").div(1000)
    df_1["Limit_Main_Time"] = df_1["Limit_Main_Time"].astype("int").div(1000)
    df_1["Protect_Main_Volt"] = df_1["Protect_Main_Volt"].astype("int").div(1000)
    df_1["Limit_Main_Curr"] = df_1["Limit_Main_Curr"].astype("int").div(1000)
    df_1["Limit_Main_Volt"] = df_1["Limit_Main_Volt"].astype("int").div(10000)
    df_1["Limit_Main_Stop_Curr"] = df_1["Limit_Main_Stop_Curr"].astype("int").div(100)
    df_1["Limit_Other_Cnd1"] = df_1["Limit_Other_Cnd1"].astype("int").div(10000)
    df_1["Protect_Main_Volt_Upper"] = (
        df_1["Protect_Main_Volt_Upper"].astype("int").div(10000)
    )
    df_1["Protect_Main_Volt_Lower"] = (
        df_1["Protect_Main_Volt_Lower"].astype("int").div(10000)
    )
    df_1[df_1 == 0] = np.nan

    return df_1


def get_remarks(ndax_file):
    """
    Function to get remarks.

    Args:
        file (str): Name of an .ndax file to read

    Returns:
        Remarks if any.

    """
    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    f = zipfile.ZipFile(ndax_file, "r")
    for x in f.namelist():
        f.extract(x, "./temdata/")
    f.close()
    files = os.listdir("./temdata/")
    tempfile = ""
    for file in files:
        if len(file.split("_")) > 2:
            tempfile = "./temdata/" + file
    file = "./temdata/Step.xml"
    with codecs.open(file, "r", "GB2312") as file:
        xml_content = file.read()
    root = ET.fromstring(xml_content)
    remark_element = root.find(".//Head_Info/Remark")
    if remark_element is not None:
        remark_value = remark_element.get("Value")
    else:
        remark_value = "Remark element not found."

    return remark_value


def get_process_name(ndax_file):
    """
    Function to get remarks.

    Args:
        file (str): Name of an .ndax file to read

    Returns:
        Step Name.

    """

    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    f = zipfile.ZipFile(ndax_file, "r")
    for x in f.namelist():
        f.extract(x, "./temdata/")
    f.close()

    tempfile = ""
    for filename in os.listdir("./temdata/"):
        if filename.endswith(".pqt"):
            tempfile = "./temdata/" + filename

    file = "./temdata/TestInfo.xml"
    with codecs.open(file, "r", "GB2312") as file:
        xml_content = file.read()
    root = ET.fromstring(xml_content)
    m1 = root.find(".//TestInfo").get("StepName")
    if m1 is not None:
        m1 = m1.strip(".xml")

    if m1 is None:
        temp_dir = "./temdata/"
        for file in os.listdir(temp_dir):
            if file.endswith(".xml"):
                file_path = os.path.join(temp_dir, file)
                with codecs.open(file_path, "r", "GB2312") as file:
                    xml_content = file.read()
                remark_element = root.find(".//Head_Info/StepName")
                if remark_element is not None:
                    m1 = remark_element.get("Value")
                    if m1 is not None:
                        m1 = m1.strip(".xml")
                else:
                    m1 = "StepName element not found."

    return m1


def get_barcode(ndax_file):
    """
    Function to get remarks.

    Args:
        file (str): Name of an .ndax file to read

    Returns:
        Barcode from xml file.

    """

    # if ndax_file.endswith('.ndax'):
    #     _,_,_, barcode  = ndax_basic.get_values(ndax_file)
    # else:
    #     raise ValueError("File passed in function is not an ndax file")

    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    f = zipfile.ZipFile(ndax_file, "r")
    for x in f.namelist():
        f.extract(x, "./temdata/")
    f.close()
    files = os.listdir("./temdata/")
    tempfile = ""
    for file in files:
        if len(file.split("_")) > 2:
            tempfile = "./temdata/" + file

    file = "./temdata/TestInfo.xml"
    with codecs.open(file, "r", "GB2312") as file:
        xml_content = file.read()
    root = ET.fromstring(xml_content)
    m1 = root.find(".//TestInfo").get("Barcode")

    if m1 is None:
        temp_dir = "./temdata/"
        for file in os.listdir(temp_dir):
            if file.endswith(".xml"):
                file_path = os.path.join(temp_dir, file)
                with codecs.open(file_path, "r", "GB2312") as file:
                    xml_content = file.read()
                remark_element = root.find(".//Head_Info/Barcode")
                if remark_element is not None:
                    m1 = remark_element.get("Value")
                else:
                    m1 = "Barcode element not found."

    return m1


def get_starttime(ndax_file):
    """
    Function to get remarks.

    Args:
        file (str): Name of an .ndax file to read

    Returns:
        Start time of the test.

    """
    # if ndax_file.endswith('.ndax'):
    #     _, _, start_time, _  = ndax_basic.get_values(ndax_file)
    # else:
    #     raise ValueError("File passed in function is not an ndax file")

    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    f = zipfile.ZipFile(ndax_file, "r")
    for x in f.namelist():
        f.extract(x, "./temdata/")
    f.close()
    files = os.listdir("./temdata/")
    tempfile = ""
    for file in files:
        if len(file.split("_")) > 2:
            tempfile = "./temdata/" + file
    file = "./temdata/TestInfo.xml"
    with codecs.open(file, "r", "GB2312") as file:
        xml_content = file.read()
    root = ET.fromstring(xml_content)
    m1 = root.find(".//TestInfo").get("StartTime")

    if os.path.isdir(r".\temdata"):
        for i in os.listdir(r".\temdata"):
            file_path = os.path.join(r".\temdata", i)
            try:
                with open(file_path, "a"):
                    pass
                os.remove(file_path)
            except PermissionError as e:
                print(f"Error: {e}")

    return m1


def rename(df):
    rec_columns1 = {
        "record_ID": "Index",
        "cycle": "Cycle",
        "step_ID": "Step",
        "step_name": "Status",
        "time_in_step": "Time",
        "voltage_V": "Voltage",
        "current_mA": "Current(A)",
        "capacity_mAh": "Capacity(Ah)",
        "energy_mWh": "Energy(Wh)",
        "timestamp": "Timestamp",
        "Validated": "Validated",
        "DCIR(mOhm)": "DCIR(mOhm)",
    }
    df["current_mA"] = df["current_mA"].div(1000)
    df["capacity_mAh"] = df["capacity_mAh"].div(1000)
    df["energy_mWh"] = df["energy_mWh"].div(1000)
    df = df.rename(columns=rec_columns1)
    return df


def get_step(df):
    """
    Function to get remarks.

    Args:
        file (str): Name of an .ndax file to read \n

    Returns:
        When passed an nda file or the records data,
        it returns Step-wise data identical to the
        step sheet in the generated excel file of a test.
    """

    try:
        if type(df) != type(pd.DataFrame()):
            df = to_df(df)
    except:
        raise ValueError(
            "Argument pushed into the function is neither an appropriate DataFrame nor a path to an ndax file"
        )
    df = rename(df)
    print(df.columns)

    rec_columns = {
        "DataPoint": "Index",
        "Cycle Index": "Cycle",
        "Step Index": "Step",
        "Status": "Status",
        "Time": "Time",
        "Voltage(V)": "Voltage",
        "Current(A)": "Current(A)",
        "Capacity(Ah)": "Capacity(Ah)",
        "Energy(Wh)": "Energy(Wh)",
        "Date": "Timestamp",
        "Validated": "Validated",
        "DCIR(mOhm)": "DCIR(mOhm)",
    }

    keycheck = ndax_basic.keys_check(df)
    if keycheck == -1:
        raise ValueError(
            "Wrong dataframe entered. Kindly check the coloumn names. Or pass the nda file."
        )

    if keycheck == 1:
        df = df.rename(columns=rec_columns)

    if "DCIR(mOhm)" not in df.keys():
        df["prev_cur"] = df["Current(A)"].shift(periods=1)
        df["prev_vol"] = df["Voltage"].shift(periods=1)
        df["DCIR(mOhm)"] = -1.0
        df.loc[((df["prev_cur"] == 0) & (df["Current(A)"] != 0)), "DCIR(mOhm)"] = (
            abs(
                (df["Voltage"] - df["prev_vol"]).astype("float64")
                / (df["Current(A)"] - df["prev_cur"]).astype("float64")
            )
            * 1000000
        )
        df.drop(columns=["prev_cur", "prev_vol"], inplace=True)

    df["prev_cur"] = df["Current(A)"].shift(periods=1)
    df["prev_vol"] = df["Voltage"].shift(periods=1)
    df["prev_step"] = df["Index"].shift(periods=1)

    temp_list = []
    complete_list = []

    for i in range(min(df["Step"]), max(df["Step"]) + 1):

        df2 = df[df["Step"] == i]
        DCIR = list(df2["DCIR(mOhm)"])[0]
        Starting_Volt = list(df2["Voltage"])[0]
        End_Voltage = list(df2["Voltage"])[-1]
        Starting_current = list(df2["Current(A)"])[0]
        End_Current = list(df2["Current(A)"])[-1]
        Capacity = list(df2["Capacity(Ah)"])[-1]
        Energy = list(df2["Energy(Wh)"])[-1]
        Starting_Date = list(df2["Timestamp"])[0]
        End_Date = list(df2["Timestamp"])[-1]
        Step_Time = timedelta(seconds=list(df2["Time"])[-1])
        step_ID = i
        Maximum_voltage = max(list(df2["Voltage"]))
        Minimum_voltage = min(list(df2["Voltage"]))
        Step_Type = list(df2["Status"])[0]
        Cycle_Index = list(df2["Cycle"])[0]

        temp_list = [
            Cycle_Index,
            step_ID,
            Step_Type,
            str(Step_Time),
            Starting_Date,
            End_Date,
            Capacity / 1000,
            Energy / 1000,
            Starting_Volt,
            End_Voltage,
            Starting_current / 1000,
            End_Current / 1000,
            Maximum_voltage,
            Minimum_voltage,
            DCIR,
        ]
        complete_list.append(temp_list)

    col_list = [
        "Cycle Index",
        "Step Number",
        "Step Type",
        "Step Time",
        "Onset Date",
        "End Date",
        "Capacity(Ah)",
        "Energy(Wh)",
        "Onset Volt.(V)",
        "End Voltage(V)",
        "Starting current(A)",
        "Termination current(A)",
        "Max Volt.(V)",
        "Min Volt(V)",
        "DCIR(mOhm)",
    ]

    df = pd.DataFrame(complete_list, columns=col_list)
    return df


def get_cycle(df):
    """
    Function to get remarks.

    Args:
        file (str): Name of an .ndax file to read \n

    Returns:
        When passed an nda file or the records data,
        it returns Cycle-wise data identical to the
        cycle sheet in the excel file of the test.
    """

    try:
        # if type(df) != type(pd.DataFrame()):
        if not isinstance(df, pd.DataFrame):
            df = ndax_basic.to_df(df)

    except Exception:
        raise ValueError(
            "Argument pushed into the function is not an appropiate DataFrame nor a path to an ndax file"
        )

    df = rename(df)
    print(df.columns)

    rec_columns = {
        "DataPoint": "Index",
        "Cycle Index": "Cycle",
        "Step Index": "Step",
        "Status": "Status",
        "Time": "Time",
        "Voltage(V)": "Voltage",
        "Current(A)": "Current(A)",
        "Capacity(Ah)": "Charge_Capacity(Ah)",
        "Energy(Wh)": "Energy(Wh)",
        "Date": "Timestamp",
        "Validated": "Validated",
        "DCIR(mOhm)": "DCIR(mOhm)",
    }

    # checking keys will cause error bcz every ndax file will have different number of columns generated
    keycheck = ndax_basic.keys_check(df)
    if keycheck == -1:
        raise ValueError(
            "Wrong dataframe entered. Kindly check the coloumn names. Or pass the nda file."
        )
    if keycheck == 1:
        df = df.rename(columns=rec_columns)

    if "DCIR(mOhm)" not in df.keys():
        df["prev_cur"] = df["Current(A)"].shift(periods=1)
        df["prev_vol"] = df["Voltage"].shift(periods=1)
        df["DCIR(mOhm)"] = -1.0
        df.loc[((df["prev_cur"] == 0) & (df["Current(A)"] != 0)), "DCIR(mOhm)"] = (
            abs((df["Voltage"] - df["prev_vol"]) / (df["Current(A)"] - df["prev_cur"]))
            * 1000000
        ).astype("float32")
        df.drop(columns=["prev_cur", "prev_vol"], inplace=True)
    temp_list = []
    complete_list = []
    chg_temp = "CCCV_Chg"  # default values
    dchg_temp = "CC_Dchg"

    step_col = list(df["Status"].unique()[1:])
    if not ((chg_temp in step_col) & (dchg_temp in step_col)):
        for col in step_col:
            if re.search("chg", col, re.IGNORECASE):
                if not re.search("d", col, re.IGNORECASE):
                    chg_temp = col
            if re.search("dchg", col, re.IGNORECASE):
                dchg_temp = col
    lastcycle_counter = 0
    if (
        df[df["Cycle"] == max(df["Cycle"])].Index.nunique()
        == df.groupby(["Cycle"]).Index.nunique().mode()[0]
    ):
        lastcycle_counter = 1

    for i in range(min(df["Cycle"]), max(df["Cycle"] + lastcycle_counter)):

        df2 = df[df["Cycle"] == i].reset_index()
        cycle = i

        starting_date = df2.at[0, "Timestamp"]
        end_date = df2.iloc[-1].at["Timestamp"]

        df_chg = df2[(df2["Status"] == chg_temp)].reset_index()
        df_dchg = df2[(df2["Status"] == dchg_temp)].reset_index()

        charging_capacity = df_chg.iloc[-1].at["Capacity(Ah)"]
        discharging_capacity = df_dchg.iloc[-1].at["Capacity(Ah)"]

        charging_energy = df_chg.iloc[-1].at["Energy(Wh)"]
        discharging_energy = df_dchg.iloc[-1].at["Energy(Wh)"]

        chg_starting_volt = df_chg.at[0, "Voltage"]
        chg_ending_volt = df_chg.iloc[-1].at["Voltage"]

        dchg_starting_volt = df_dchg.at[0, "Voltage"]
        dchg_ending_volt = df_dchg.iloc[-1].at["Voltage"]

        chg_starting_current = df_chg.at[0, "Current(A)"]
        chg_ending_current = df_chg.iloc[-1].at["Current(A)"]

        dchg_starting_current = df_dchg.at[0, "Current(A)"]
        dchg_ending_current = df_dchg.iloc[-1].at["Current(A)"]

        chgtime = df_chg.iloc[-1].at["Time"]
        charging_time = "{:02}:{:02}:{:02}".format(
            int(chgtime // 3600), int(chgtime % 3600 // 60), int(chgtime % 60)
        )
        dchgtime = df_dchg.iloc[-1].at["Time"]
        discharging_time = "{:02}:{:02}:{:02}".format(
            int(dchgtime // 3600), int(dchgtime % 3600 // 60), int(dchgtime % 60)
        )

        dcir_cyl = df2.loc[df2["DCIR(mOhm)"] > 0, "DCIR(mOhm)"]

        DCIR_avg = dcir_cyl.mean()

        temp_list = [
            cycle,
            starting_date,
            end_date,
            charging_capacity,
            discharging_capacity,
            charging_energy,
            discharging_energy,
            charging_time,
            discharging_time,
            chg_starting_volt,
            dchg_starting_volt,
            chg_ending_volt,
            dchg_ending_volt,
            chg_starting_current,
            dchg_starting_current,
            chg_ending_current,
            dchg_ending_current,
            DCIR_avg,
        ]

        complete_list.append(temp_list)

    df3 = pd.DataFrame(
        complete_list,
        columns=[
            "Cycle Index",
            "Onset Date",
            "End Date",
            "Chg. Cap.(Ah)",
            "DChg. Cap.(Ah)",
            "Chg. Energy(Wh)",
            "DChg. Energy_(Wh)",
            "Chg_Time(hh:mm:ss)",
            "DChg_Time(hh:mm:ss)",
            "Chg_Onset_Volt_(V)",
            "DChg_Onset_Volt_(V)",
            "End_of_Chg_Volt(V)",
            "End_of_DChg_Volt(V)",
            "Chg_Oneset_Current(A)",
            "DChg_Oneset_Curent(A)",
            "End_of_Chg_Current(A)",
            "End_of_DChg_Current(A)",
            "DCIR(mOhm)",
        ],
    )
    return df3


def get_recipe(df):
    """
    Function to get remarks.

    Args:
        df : Dataframe generated by get_records() function  \n

    Returns:
        When passed an nda file or the records data,
        it prints the recipe(s) with the cycle numbers by analysing the values of the data.
        It also returns two dictionaries. First one with the cycle numbers denoting the cycle numbers corresponding to the recipe.
        And the second one with the recipe(s) based off the data and its values
    """

    try:
        if type(df) != type(pd.DataFrame()):
            df = ndax_basic.to_df(df)
    except:
        raise ValueError(
            "Argument pushed into the function is neither an appropriate DataFrame nor a path to an ndax file"
        )

    df = rename(df)

    dict_voltage = {}
    dict_recipe = {}
    dict_rest = {}
    dict_current = {}
    dict_step = {}
    dict_cycle = {}
    recipe_cycle = []
    dict_stepid = {}
    dict_cutoff_curr = {}
    dict_cutoff_vol = {}
    chg_temp = ""
    dchg_temp = ""
    step_col = list(df["Status"].unique()[1:])
    for col in step_col:
        if re.search("_chg", col, re.IGNORECASE):
            chg_temp = col
        if re.search("_dchg", col, re.IGNORECASE):
            dchg_temp = col

    for i in range(min(df["Cycle"]), max(df["Cycle"])):
        # For non-validated files

        df2 = df[df["Cycle"] == i]
        recipe_cycle.append(i)
        dict_recipe[i] = []
        dict_rest[i] = []
        dict_voltage[i] = []
        dict_current[i] = []
        dict_step[i] = []
        dict_cycle[i] = []
        dict_stepid[i] = []
        dict_cutoff_curr[i] = []
        dict_cutoff_vol[i] = []
        for j in range(min(df2["Step"]), max(df2["Step"]) + 1):
            df3 = df2[df2["Step"] == j]
            dict_recipe[i].append(df3["Status"].iloc[0])

            dict_step[i].append(j)

            dict_cycle[i].append(i)

            dict_stepid[i].append(j)

            if df3["Status"].iloc[0] == "Rest":
                dict_rest[i].append(timedelta(seconds=list(df3["Time"])[-1]))
            else:
                dict_rest[i].append(0)

            if df3["Status"].iloc[0] == chg_temp:
                dict_voltage[i].append(round(df3["Voltage"].max(), 2))
                dict_cutoff_vol[i].append(0)
            elif df3["Status"].iloc[0] == dchg_temp:
                dict_voltage[i].append(round(df3["Voltage"].min(), 2))
                dict_cutoff_vol[i].append(round(list(df3["Voltage"])[-1], 2))
            else:
                dict_voltage[i].append(0)
                dict_cutoff_vol[i].append(0)

            if df3["Status"].iloc[0] == chg_temp:
                dict_current[i].append(round(df3["Current(A)"].iloc[0] / 1000, 2))
                dict_cutoff_curr[i].append(round(list(df3["Current(A)"])[-1] / 1000, 2))
            elif df3["Status"].iloc[0] == dchg_temp:
                dict_current[i].append(round(df3["Current(A)"].iloc[0] / 1000, 2))
                dict_cutoff_curr[i].append(0)
            else:
                dict_current[i].append(0)
                dict_cutoff_curr[i].append(0)

    recipe = []
    rest = []
    voltage = []
    current = []
    cycle = []
    stepid = []
    cutoff_curr = []
    cutoff_vol = []

    for i in recipe_cycle:
        for j in range(len(dict_recipe[i])):
            recipe.append(dict_recipe[i][j])
            rest.append(dict_rest[i][j])
            voltage.append(dict_voltage[i][j])
            current.append(dict_current[i][j])
            cycle.append(dict_cycle[i][j])
            stepid.append(dict_stepid[i][j])
            cutoff_curr.append(dict_cutoff_curr[i][j])
            cutoff_vol.append(dict_cutoff_vol[i][j])

    df_recipe = pd.DataFrame(
        zip(
            np.array(stepid).reshape(-1, 1),
            np.array(cycle).reshape(-1, 1),
            np.array(recipe).reshape(-1, 1),
            np.array(voltage).reshape(-1, 1),
            np.array(current).reshape(-1, 1),
            np.array(rest).reshape(-1, 1),
            np.array(cutoff_curr).reshape(-1, 1),
            np.array(cutoff_vol).reshape(-1, 1),
        ),
        columns=[
            "Step",
            "Cycle",
            "Status",
            "Voltage",
            "Current(A)",
            "Rest_time",
            "Cutoff_current",
            "Cutoff_voltage",
        ],
    )

    for col in df_recipe.columns:
        df_recipe[col] = df_recipe[col].apply(lambda x: x[0])

    # df_recipe.replace(r'nan',r' ',regex=True,inplace=True)

    recipe_unmatch = [1]
    for i in range(len(recipe_cycle) - 1):
        # df_temp1=df_recipe.groupby('Cycle').get_group(recipe_cycle[i])
        df_temp1 = df_recipe[df_recipe["Cycle"] == recipe_cycle[i]]
        # df_temp2=df_recipe.groupby('Cycle').get_group(recipe_cycle[i+1])
        df_temp2 = df_recipe[df_recipe["Cycle"] == recipe_cycle[i + 1]]
        df_temp1 = df_temp1.drop(["Cycle", "Step"], axis=1)
        df_temp2 = df_temp2.drop(["Cycle", "Step"], axis=1)
        if (df_temp1.reset_index(drop=True).shape) == (
            df_temp2.reset_index(drop=True).shape
        ) and ndax_basic.df_diff(
            df_temp1.reset_index(drop=True), df_temp2.reset_index(drop=True)
        ) == 1:
            continue
        else:
            recipe_unmatch.append(recipe_cycle[i + 1])
    recipe_unmatch.append(df_recipe["Cycle"].max())
    recipe_unmatch = sorted(list(set(recipe_unmatch)))

    dict_consecutive = {}
    for i in range(len(recipe_unmatch) - 1):
        dict_consecutive[recipe_unmatch[i]] = recipe_unmatch[i + 1]

    # print('Done')

    dict = {}
    for i in range(len(recipe_unmatch) - 1):
        dict[recipe_unmatch[i]] = []
        # df1=df_recipe.groupby('Cycle').get_group(recipe_unmatch[i]).reset_index(drop=True)
        df1 = df_recipe[df_recipe["Cycle"] == recipe_unmatch[i]].reset_index(drop=True)
        df1 = df1.drop(["Cycle", "Step"], axis=1)
        for j in range(i + 1, len(recipe_unmatch)):
            # df2=df_recipe.groupby('Cycle').get_group(recipe_unmatch[j]).reset_index(drop=True)
            df2 = df_recipe[df_recipe["Cycle"] == recipe_unmatch[j]].reset_index(
                drop=True
            )
            df2 = df2.drop(["Cycle", "Step"], axis=1)
            if df1.shape == df2.shape and ndax_basic.df_diff(df1, df2) == 1:
                dict[recipe_unmatch[i]].append(recipe_unmatch[j])

    dict_temp = {}
    for i in dict:
        if i in dict_consecutive:
            dict_temp[i] = [[i, dict_consecutive[i] - 1]]
            if len(dict[i]) != 0:
                for j in dict[i]:
                    if j in dict_consecutive:
                        dict_temp[i].append([j, dict_consecutive[j] - 1])
                        del dict_consecutive[j]

    # print(dict_temp)
    iter = 1
    dict_recipe_range = {}
    for x in dict_temp:
        dict_recipe_range["Recipe-{k}".format(k=x)] = dict_temp[x]
        iter += 1
    print(dict_recipe_range)

    arr = []
    dict_ = {}
    k = 0
    temp_list = list(dict_temp.keys())
    for i in range(len(temp_list)):
        k += 1
        # df_temp=df_recipe.groupby('Cycle').get_group(recipe_unmatch[i])
        df_temp = df_recipe[df_recipe["Cycle"] == temp_list[i]]
        df_temp = df_temp.drop(["Cycle", "Step"], axis=1).reset_index(drop=True)
        arr.append(df_temp)
        print("Recipe-{k}".format(k=k))
        dict_["Recipe-{k}".format(k=k)] = df_temp
        displayhook(df_temp)
    return dict_recipe_range, dict_
