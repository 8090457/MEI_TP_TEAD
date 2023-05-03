import pandas as pd
import matplotlib.pyplot as plt


def outliersSimple(df: pd.DataFrame, column_to_filter: str, filterValue: int):
    _, axs = plt.subplots(1, 2)

    axs[0].set_title(f"{column_to_filter} (prev)")
    axs[0].boxplot(df[column_to_filter])

    filtered = df[df[column_to_filter] <= filterValue]
    axs[1].set_title(f"{column_to_filter} <= {filterValue}")
    axs[1].boxplot(filtered[column_to_filter])

    plt.show()


def outliersComplex(df: pd.DataFrame, column_to_filter: str, condition_str: str):
    # convert string to boolean expression
    condition = eval(f"lambda x: x {condition_str}")

    _, axs = plt.subplots(1, 2)

    axs[0].set_title(f"{column_to_filter} (prev)")
    axs[0].boxplot(df[column_to_filter])

    # apply filter condition to dataframe
    filtered = df[condition(df[column_to_filter])]

    axs[1].set_title(f"{column_to_filter} {condition_str}")
    axs[1].boxplot(filtered[column_to_filter])

    plt.show()


def outliersWithPieChart(df: pd.DataFrame, column_to_filter: str, condition_str: str):
    outliersComplex(df, column_to_filter, condition_str)
    # convert string to boolean expression
    condition = eval(f"lambda x: x {condition_str}")

    counts = df[column_to_filter].value_counts()

    count = counts.to_list()
    index = counts.index.to_list()

    filtered = df[condition(df[column_to_filter])]
    counts2 = filtered[column_to_filter].value_counts()

    count2 = counts2.to_list()
    index2 = counts2.index.to_list()

    _, axs2 = plt.subplots(1, 2)

    nl1 = []
    for i in range(len(count)):
        s = f"{index[i]} - {count[i]}"
        nl1.append(s)

    nl2 = []
    for i in range(len(count2)):
        s = f"{index2[i]} - {count2[i]}"
        nl2.append(s)

    axs2[0].pie(counts, autopct="%1.0f%%")

    # move legend to the right
    axs2[0].legend(nl1, loc="right", bbox_to_anchor=(1.5, 0.5))

    axs2[1].pie(counts2, autopct="%1.0f%%")

    # move legend to the right
    axs2[1].legend(nl2, loc="right", bbox_to_anchor=(1.5, 0.5))

    plt.show()
