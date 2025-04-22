import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Configurações gerais
sns.set_style("whitegrid")
sns.set_palette("pastel")

def plot_classificacao_contas(df):
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.countplot(
        data=df,
        x="categoria",
        order=["A", "B", "C", "D"],
        edgecolor="black",
        linewidth=1.2,
        ax=ax
    )
    plt.title("", fontsize=16, fontweight="bold", pad=20)
    plt.xlabel("Classe da Conta", fontsize=14, labelpad=10)
    plt.ylabel("Quantidade", fontsize=14, labelpad=10)
    ax.tick_params(axis="both", which="major", labelsize=12)
    sns.despine()

    for p in ax.patches:
        ax.annotate(
            f"{int(p.get_height())}",
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha="center", va="center",
            fontsize=12, color="black",
            xytext=(0, 8),
            textcoords="offset points"
        )
    st.pyplot(fig)

def plot_top_10_cidades(df):
    fig, ax = plt.subplots(figsize=(12, 7))
    sns.barplot(
        data=df,
        x="location",
        y="num_trans",
        edgecolor="black",
        linewidth=1.2,
        ax=ax
    )
    plt.title("", fontsize=18, fontweight="bold", pad=20)
    plt.xlabel("", fontsize=14, labelpad=10)
    plt.ylabel("Número de Transações", fontsize=14, labelpad=10)
    plt.xticks(rotation=45, ha="right", fontsize=12)
    plt.yticks(fontsize=12)
    sns.despine()

    for p in ax.patches:
        ax.annotate(
            f"{int(p.get_height())}",
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha="center", va="center",
            fontsize=12, color="black",
            xytext=(0, 8),
            textcoords="offset points"
        )
    st.pyplot(fig)

def plot_transacoes_dia(df):
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(
        data=df,
        x="data",
        y="trans",
        marker="o",
        markersize=8,
        linewidth=2.5,
        color="purple",
        ax=ax
    )
    plt.title("", fontsize=18, fontweight="bold", pad=20)
    plt.xlabel("Data", fontsize=14, labelpad=10)
    plt.ylabel("Número de Transações", fontsize=14, labelpad=10)
    plt.xticks(rotation=45, ha="right", fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(visible=True, linestyle="--", linewidth=0.7, alpha=0.7)
    sns.despine()
    st.pyplot(fig)

def plot_anormalidades(df):
    df_melted = df.melt(
        id_vars="transation_id",
        value_vars=["is_location_suspicious", "is_amount_suspicious"]
    )
    df_melted = df_melted[df_melted["value"] == 1]

    rename_dict = {
        "is_location_suspicious": "Localização Suspeita",
        "is_amount_suspicious": "Valor Suspeito"
    }
    df_melted["variable"] = df_melted["variable"].map(rename_dict)

    fig, ax = plt.subplots(figsize=(8, 5))
    sns.countplot(
        data=df_melted,
        x="variable",
        edgecolor="black",
        linewidth=1.2,
        ax=ax
    )

    ax.set_title("", fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Tipo de Anormalidade", fontsize=14, labelpad=10)
    ax.set_ylabel("Quantidade", fontsize=14, labelpad=10)
    ax.tick_params(axis="both", labelsize=12)
    sns.despine()

    for p in ax.patches:
        ax.annotate(
            f"{int(p.get_height())}",
            (p.get_x() + p.get_width() / 2., p.get_height()),
            ha="center",
            va="center",
            fontsize=12,
            color="black",
            xytext=(0, 8),
            textcoords="offset points"
        )

    st.pyplot(fig)


def show_summary_table(df):
    st.dataframe(df, use_container_width=True)
    
def plot_benchmarkingCSV(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Linha do tempo médio
    sns.lineplot(
        data=df,
        x="numThreads",
        y="meanTime",
        marker="o",
        markersize=8,
        linewidth=2.5,
        color="blue",
        label="Tempo Médio",
        ax=ax
    )

    # Área de intervalo Min/Max
    ax.fill_between(
        df["numThreads"],
        df["minTime"],
        df["maxTime"],
        color="lightblue",
        alpha=0.4,
        label="Min/Max Intervalo"
    )

    ax.set_title("Tempo de Execução vs Número de Threads", fontsize=18, fontweight="bold", pad=20)
    ax.set_xlabel("Número de Threads", fontsize=14, labelpad=10)
    ax.set_ylabel("Tempo (ms)", fontsize=14, labelpad=10)
    ax.legend(fontsize=12)
    ax.grid(visible=True, linestyle="--", linewidth=0.7, alpha=0.7)
    ax.tick_params(axis="both", which="major", labelsize=12)
    sns.despine()
    
    st.pyplot(fig)
    
def plot_benchmarkingDB(df):
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Linha do tempo médio
    sns.lineplot(
        data=df,
        x="numThreads",
        y="meanTime",
        marker="o",
        markersize=8,
        linewidth=2.5,
        color="blue",
        label="Tempo Médio",
        ax=ax
    )

    # Área de intervalo Min/Max
    ax.fill_between(
        df["numThreads"],
        df["minTime"],
        df["maxTime"],
        color="lightblue",
        alpha=0.4,
        label="Min/Max Intervalo"
    )

    ax.set_title("Tempo de Execução vs Número de Threads", fontsize=18, fontweight="bold", pad=20)
    ax.set_xlabel("Número de Threads", fontsize=14, labelpad=10)
    ax.set_ylabel("Tempo (ms)", fontsize=14, labelpad=10)
    ax.legend(fontsize=12)
    ax.grid(visible=True, linestyle="--", linewidth=0.7, alpha=0.7)
    ax.tick_params(axis="both", which="major", labelsize=12)
    sns.despine()
    
    st.pyplot(fig)


# Título
st.markdown(
    """
    <style>
    .center-title {
        text-align: center;
    }
    </style>
    <h1 class="center-title">Banco</h1>
    """,
    unsafe_allow_html=True
)

if st.button("🔄 Refresh"):
    st.success("Dados atualizados!")

    df_classificacao = pd.read_csv("output/classified_accounts.csv")
    df_cidades = pd.read_csv("output/top_10_cities.csv")
    #df_transacoes = pd.read_csv("output/transacoes_dia.csv")
    df_anormalidades = pd.read_csv("output/abnormal_transactions.csv")
    df_summary = pd.read_csv("output/summary_stats.csv")
    df_benchmarkingCSV = pd.read_csv("data/benchmarkingCSV.csv")
    df_benchmarkingDB = pd.read_csv("data/benchmarkingDB.csv")

    # Plots
    st.header("Distribuição das Classes de Conta")
    st.markdown("""
    **Classes Presentes:** 
    - **A**: se a média de transações for superior a 500.
    - **B**: se a média de transações estiver entre 200 e 500 **e** o saldo for maior que 10.000.
    - **C**: se a média de transações estiver entre 200 e 500 **e** o saldo for menor ou igual a 10.000.
    - **D**: se a média de transações for inferior a 200.
    """)
    plot_classificacao_contas(df_classificacao)

    st.header("Top 10 Capitais com mais Transações")
    plot_top_10_cidades(df_cidades)

    st.header("Média de Transações por Dia")
    #plot_transacoes_dia(df_transacoes)
    
    st.header("Transações Anormais")
    plot_anormalidades(df_anormalidades)

    st.header("Summary das Transações")
    show_summary_table(df_summary)
    
    st.header("Benchmarking CSV")
    plot_benchmarkingCSV(df_benchmarkingCSV)
    
    st.header("Benchmarking DB")
    plot_benchmarkingCSV(df_benchmarkingDB)

else:
    st.info("Clique no botão acima para carregar os gráficos.")
