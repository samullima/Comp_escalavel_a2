import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Configurações gerais do seaborn
sns.set_style("whitegrid")
sns.set_palette("pastel")

def plot_classificacao_contas(df):
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.countplot(
        data=df,
        x="account_class",
        order=["A", "B", "C", "D"],
        edgecolor="black",
        linewidth=1.2,
        ax=ax
    )
    plt.title("Distribuição das Classes de Conta", fontsize=16, fontweight="bold", pad=20)
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
        x="cidade",
        y="quantidade",
        edgecolor="black",
        linewidth=1.2,
        ax=ax
    )
    plt.title("Quantidade de Transações por Cidade", fontsize=18, fontweight="bold", pad=20)
    plt.xlabel("", fontsize=14, labelpad=10)
    plt.ylabel("Quantidade", fontsize=14, labelpad=10)
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
    plt.title("Média de Transações por Dia", fontsize=18, fontweight="bold", pad=20)
    plt.xlabel("Data", fontsize=14, labelpad=10)
    plt.ylabel("Número de Transações", fontsize=14, labelpad=10)
    plt.xticks(rotation=45, ha="right", fontsize=12)
    plt.yticks(fontsize=12)
    plt.grid(visible=True, linestyle="--", linewidth=0.7, alpha=0.7)
    sns.despine()
    st.pyplot(fig)

def show_summary_table(df):
    st.header("Resumo Estatístico")
    st.dataframe(df, use_container_width=True)

st.title("Banco")

if st.button("🔄 Refresh"):
    st.success("Dados atualizados!")

    df_classificacao = pd.read_csv("classificacao_contas.csv")
    df_cidades = pd.read_csv("top_10_cidades.csv")
    df_transacoes = pd.read_csv("transacoes_dia.csv")
    df_summary = pd.read_csv("summary_states.csv")

    # Plots
    st.header("Distribuição das Classes de Conta")
    plot_classificacao_contas(df_classificacao)

    st.header("Quantidade de Transações por Cidade")
    plot_top_10_cidades(df_cidades)

    st.header("Média de Transações por Dia")
    plot_transacoes_dia(df_transacoes)

    st.header("Summary")
    show_summary_table(df_summary)

else:
    st.info("Clique no botão acima para carregar os gráficos.")
