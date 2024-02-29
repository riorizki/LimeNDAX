import codecs
import xml.etree.ElementTree as ET
import os
import zipfile
import struct
from datetime import datetime
from sys import displayhook
import re
import logging

ILLEGAL_CHARACTERS_RE = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")

state_dict = {
    1: "CC_Chg",
    2: "CC_Dchg",
    3: "CV_Chg",
    4: "Rest",
    5: "Cycle",
    # 6: '6',
    7: "CCCV_Chg",
    8: "CP_Dchg",
    9: "CP_Chg",
    10: "CR_Dchg",
    # 11: '11',
    # 12: '12',
    13: "Pause",
    # 14: '14',
    # 15: '15',
    16: "Pulse",
    17: "SIM",
    # 18: '18',
    19: "CV_Dchg",
    20: "CCCV_Dchg",
    21: "Control",
    26: "CPCV_Dchg",
    27: "CPCV_Chg",
}


def single_validator(list):

    if list[0] < 1 or list[1] < 1 or list[2] < 1 or list[5] < 1.5:
        return False
    return True


def byte_to_list(bytes, oldlist=[]):
    # Extract fields from byte string
    [Index, Cycle] = struct.unpack("<II", bytes[8:16])
    [Step] = struct.unpack("<B", bytes[16:17])
    [Status] = struct.unpack("<B", bytes[17:18])
    [Time] = struct.unpack("<Q", bytes[23:31])
    [Voltage, Current] = struct.unpack("<ii", bytes[31:39])
    [Charge_capacity, Discharge_capacity] = struct.unpack("<qq", bytes[43:59])
    [Charge_energy, Discharge_energy] = struct.unpack("<qq", bytes[59:75])
    [Y, M, D, h, m, s] = struct.unpack("<HBBBBB", bytes[75:82])
    [Range] = struct.unpack("<i", bytes[82:86])

    multiplier_dict = {
        -300000: 1e-2,
        -200000: 1e-2,
        -100000: 1e-2,
        -60000: 1e-2,
        -30000: 1e-2,
        -50000: 1e-2,
        -20000: 1e-2,
        -12000: 1e-2,
        -10000: 1e-2,
        -6000: 1e-2,
        -5000: 1e-2,
        -3000: 1e-2,
        -2000: 1e-2,
        -1000: 1e-2,
        -500: 1e-3,
        -100: 1e-3,
        -50: 1e-4,
        -25: 1e-4,
        -1: 1e-5,
        0: 0,
        10: 1e-3,
        100: 1e-2,
        112: 1e-2,
        200: 1e-2,
        1000: 1e-1,
        6000: 1e-1,
        10000: 1e-1,
        12000: 1e-1,
        50000: 1e-1,
        60000: 1e-1,
        100000: 1e-1,
    }

    multiplier = multiplier_dict[Range]

    # Create a record
    list = [
        Index,
        Cycle + 1,
        Step,
        state_dict[Status],
        Time / 1000,
        Voltage / 10000,
        Current * multiplier / 1000,
        abs(Charge_capacity - Discharge_capacity) * multiplier / 3600000,
        abs(Charge_energy - Discharge_energy) * multiplier / 3600000,
        datetime(Y, M, D, h, m, s),
    ]
    list.append(single_validator(list))
    return list


def keys_check(df):
    """
    Internal Function. Do not use.

    To check for columns of the df being passed into the step, cycle, recipe functions.
    """
    counter1 = True
    col_list1 = [
        "Index",
        "Cycle",
        "Step",
        "Status",
        "Time",
        "Voltage",
        "Current(A)",
        "Capacity(Ah)",
        "Energy(Wh)",
        "Timestamp",
        # 'T',
        "DCIR(mOhm)",
    ]

    col_list2 = [
        "DataPoint",
        "Cycle Index",
        "Step Index",
        "Status",
        "Time",
        "Voltage(V)",
        "Current(A)",
        "Capacity(Ah)",
        "Energy(Wh)",
        "Date",
        #  'Temperature',
        "DCIR(mOhm)",
    ]

    for i in col_list1:
        # print(i)
        if i not in df.keys():

            counter1 = False
            break
    if counter1 == True:
        return 0

    counter2 = True
    for i in col_list2:
        if i not in df.keys():
            counter2 = False
    if counter2 == True:
        return 1

    if counter1 == False & counter2 == False:
        return -1
    return -1


def aux_to_list(bytes):
    [temperature] = struct.unpack("<h", bytes[41:43])
    [Index] = struct.unpack("<I", bytes[8:12])
    # [Cycle] = struct.unpack('<I', bytes[12:16])

    list = [Index, temperature / 10]
    return list


rec_columns = [
    "Index",
    "Cycle",
    "Step",
    "Status",
    "Time",
    "Voltage",
    "Current(A)",
    "Capacity(Ah)",
    "Energy(Wh)",
    "Timestamp",
    "Validated",
]

aux_columns = ["Index", "T"]


def count_changes(series):
    """Enumerate the number of value changes in a series"""
    a = series.diff()
    a.iloc[0] = 1
    # a.iloc[-1] = 0
    return (abs(a) > 0).cumsum()


# Function to find distinct-recipes
def df_diff(df1, df2):
    """
    verifing code in recipe. Do not use.
    """
    vol_diff = abs(df1["Voltage"]) - abs(df2["Voltage"])
    curr_diff = abs(df1["Current(A)"]) - abs(df2["Current(A)"])
    cutoff_curr_diff = abs(df1["Cutoff_current"]) - abs(df2["Cutoff_current"])
    cutoff_vol_diff = abs(df1["Cutoff_voltage"]) - abs(df2["Cutoff_voltage"])
    if list(df1["Status"]) != list(df2["Status"]):
        return -1
    if list(df1["Rest_time"]) != list(df2["Rest_time"]):
        return -1
    combined = []
    for i in range(len(vol_diff)):
        combined.append(vol_diff[i])
        combined.append(curr_diff[i])
        combined.append(cutoff_curr_diff[i])
        combined.append(cutoff_vol_diff[i])
    for i in combined:
        if abs(i) > 0.05:
            return -1
    return 1


def aux_bytes65(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux] = struct.unpack("<B", bytes[3:4])
    [Index] = struct.unpack("<I", bytes[8:12])
    [T] = struct.unpack("<h", bytes[41:43])
    [V] = struct.unpack("<i", bytes[31:35])

    return [Index, Aux, V / 10000, T / 10]


def aux_bytes74(bytes):
    """Helper function for intepreting auxiliary records"""
    [Aux] = struct.unpack("<B", bytes[3:4])
    [Index] = struct.unpack("<I", bytes[8:12])
    [V] = struct.unpack("<i", bytes[31:35])
    [T, t] = struct.unpack("<hh", bytes[41:45])

    return [Index, Aux, V / 10000, T / 10, t / 10]


def get_values(ndax_file):
    """
    Extract all the data about remark, start, time

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

    m2 = root.find(".//TestInfo").get("StepName")
    stepname = m2.strip(".xml")

    start_time = root.find(".//TestInfo").get("StartTime")

    barcode = root.find(".//TestInfo").get("Barcode")
    if barcode is None:
        for file in tempfile.endswith(".xlm"):
            with codecs.open(file, "r", "GB2312") as file:
                xml_content = file.read()
            root = ET.fromstring(xml_content)
            barcode = root.find(".//TestInfo").get("Barcode")

    return remark_value, stepname, start_time, barcode


def validate_timegap(df):
    """
    #:Check if there is a gap due to electricity cutting off and thus if there are errors there.
    #:Can check if removing those records makes the dataframe pass the validation
    """
    df["prev_step"] = df["Step"].shift(periods=1)

    df["prev_tis"] = df["Time"].shift(periods=1)
    df["prev_tstamp"] = df["Timestamp"].shift(periods=1)
    df["prev_tis"] = df["Time"] - df["prev_tis"]
    df["prev_tstamp"] = df["Timestamp"] - df["prev_tstamp"]
    df["prev_tstamp"] = df["prev_tstamp"].dt.total_seconds()

    # todo prev_tis and prev_tstamp now represent difference. of the time in step and timestamp
    df.loc[
        (
            (abs(df["prev_tis"] - df["prev_tstamp"]) > 5)
            & ((df["Step"] == df["prev_step"]) & (df["Time"] != 0))
        ),
        "Validated",
    ] = True
    df.drop(columns=["prev_tis", "prev_tstamp", "prev_step"], inplace=True)


def validator_fab(df):
    """
    Args:
      df: The dataframe to be validated
    Returns:
      True if the data that's decoded is valid, and False if it is not.

    It checks if the dataframe is valid.

    The function takes in a dataframe and a capacity_nom.

    It checks if the Index is monotonically increasing and starts at 1.

    It checks if the Cycle starts at 1.

    It checks if the Step is monotonically increasing and starts at 1.

    """
    validate_timegap(df)
    if False in df.Validated.values:
        print("Df validation failed. Kindly check.")
        logging.info(
            "This df has failed the basic validation after ignoring the timegaps' records"
        )
        return False

    df.loc[
        (df["Step"] == "Rest") & (df["Current(A)"] == 0) & (df["Time"] != 0),
        "Validated",
    ] = False
    if (df["Index"].min() != 1) or (not df["Index"].is_monotonic_increasing):
        logging.info("Index error. Min Index is: " + str(df["Index"].min()))
        return False
    if df["Cycle"].min() != 1:
        logging.info("Cycle error. Min cycle is: " + str(df["Cycle"].min()))
        return False
    if df["Step"].min() != 1 or (not df["Step"].is_monotonic_increasing):
        logging.info("Step error")
        return False


def main_validator(df, capacity_nom):
    """
    Args:
      df: The dataframe to be validated
      capacity_nom: The nominal capacity of the battery.

    Returns:
      True if the data that's decoded is valid, and False if it is not.

    It checks if the dataframe is valid.

    The function takes in a dataframe and a capacity_nom.

    It checks if the Index is monotonically increasing and starts at 1.

    It checks if the Cycle starts at 1.

    It checks if the Step is monotonically increasing and starts at 1.

    It checks if the voltage is greater than 2.

    It checks if the capacity is greater than 0.

    It checks if the capacity is less than 1500 times the capacity_nom.

    It checks if the current is less than 1600 times the capacity_nom.

    It checks if the process name, start time, and barcode are valid.

    It checks if the barcode is 12 characters long.

    """

    validate_timegap(df)
    if False in df.Validated.values:
        print("Df validation failed. Kindly check.")
        logging.info(
            "This df has failed the basic validation after ignoring the timegaps' records"
        )
        return False

    #!Could return numbers instead of True False to indicate what error we have. And then check for electricity timegap with that error
    # basic validation is same as nda because file will not change the electrochemical data.
    df.loc[
        (df["Step"] == "Rest") & (df["Current(A)"] == 0) & (df["Time"] != 0),
        "Validated",
    ] = False
    if (df["Index"].min() != 1) or (not df["Index"].is_monotonic_increasing):
        logging.info("Index error. Min Index is: " + str(df["Index"].min()))
        return False
    if df["Cycle"].min() != 1:
        logging.info("Cycle error. Min cycle is: " + str(df["Cycle"].min()))
        return False
    if df["Step"].min() != 1 or (not df["Step"].is_monotonic_increasing):
        logging.info("Step error")
        return False
    if (df["Voltage"].min() < 2) and (df[df["Status"] == "SIM"].shape[0] == 0):
        logging.info("Voltage error. Min voltage is: " + str(df["Voltage"].min()))
        return False
    if (df["Charge_Energy(Wh)"].min() < 0) and (
        df[df["Status"] == "SIM"].shape[0] == 0
    ):
        logging.info(
            "Negative Energy error. Min energy is: "
            + str(df["Charge_Energy(Wh)"].min())
        )
        return False

    if (df["Charge_Capacity(Ah)"].min() < 0) and (
        df[df["Status"] == "SIM"].shape[0] == 0
    ):
        logging.info(
            "Negative Capacity error. Min capacity is: "
            + str(df["Charge_Capacity(Ah)"].min())
        )
        return False

    if df["Charge_Capacity(Ah)"].max() > (1500 * capacity_nom):
        logging.info(
            "Max Capacity error. Max capacity is: "
            + str(df["Charge_Capacity(Ah)"].max())
        )
        return False
    if df["Current(A)"].max() > (1600 * capacity_nom):
        logging.info("Current error. Max current is: " + str(df["Current(A)"].max()))
        return False
    if all(ele in df.keys() for ele in ["Process Name", "Start Time", "barcode"]):
        if ILLEGAL_CHARACTERS_RE.search(df["Process Name"].unique()[0]):
            logging.info(
                "Process name error. The decoded Process name is: "
                + str(df["Process Name"].unique()[0])
            )
            return False
        if ILLEGAL_CHARACTERS_RE.search(df["Start Time"].unique()[0]):
            logging.info(
                "Start Time error. The decoded Start Time is: "
                + str(df["Start Time"].unique()[0])
            )
            return False
        if ILLEGAL_CHARACTERS_RE.search(df["barcode"].unique()[0]):
            logging.info(
                "Barcode error. The decoded Barcode is: "
                + str(df["barcode"].unique()[0])
            )
            return False
        if len(df["barcode"].unique()[0]) != 12:
            logging.info(
                "Barcode length error. The decoded Barcode is: "
                + str(df["barcode"].unique()[0])
            )
            return False

    return True
