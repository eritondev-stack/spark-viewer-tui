import random
from datetime import datetime, timedelta

from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Vertical
from textual.widgets import TextArea, DataTable, Static, Tree


class Sidebar(Static):
    """Barra lateral da aplicação"""

    def compose(self) -> ComposeResult:
        yield Static("🎨 Minha App Textual", id="title")
        yield Static("────────────────────", id="separator")
        tree: Tree[str] = Tree("Databases", id="db-tree")
        tree.root.expand()

        # Banco 1
        pg = tree.root.add("PostgreSQL", expand=True)
        pg.add_leaf("users")
        pg.add_leaf("orders")
        pg.add_leaf("products")
        pg.add_leaf("payments")

        # Banco 2
        mysql = tree.root.add("MySQL", expand=True)
        mysql.add_leaf("customers")
        mysql.add_leaf("invoices")
        mysql.add_leaf("inventory")

        # Banco 3
        mongo = tree.root.add("MongoDB", expand=True)
        mongo.add_leaf("sessions")
        mongo.add_leaf("logs")
        mongo.add_leaf("analytics")

        yield tree


class TextualApp(App):
    """Aplicação principal Textual"""

    CSS = """
    Screen {
        layout: horizontal;
        background: transparent;
    }

    Sidebar {
        width: 30;
        height: 1fr;
        border: solid $primary;
        padding: 1;
        background: transparent;
    }

    Sidebar > Static {
        background: transparent;
    }

    #title {
        text-style: bold;
        color: $primary;
        text-align: center;
        background: transparent;
    }

    #separator {
        color: $primary;
        background: transparent;
    }

    #db-tree {
        background: transparent;
        padding: 0;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-color: magenta;
        scrollbar-color-hover: magenta 80%;
        scrollbar-color-active: magenta 60%;
        scrollbar-size: 1 2;
    }

    Tree > .tree--cursor {
        background: $primary;
    }

    Tree > .tree--guides {
        color: $primary;
    }

    Tree > .tree--guides-hover {
        color: magenta;
    }

    #main-container {
        width: 1fr;
        height: 1fr;
        padding: 0;
        background: transparent;
    }

    Vertical {
        background: transparent;
    }

    Container {
        background: transparent;
    }

    TextArea {
        height: 10;
        border: solid $primary;
        padding: 0;
        background: transparent;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-color: magenta;
        scrollbar-color-hover: magenta 80%;
        scrollbar-color-active: magenta 60%;
        scrollbar-size: 1 2;
    }

    TextArea > .text-area--cursor-line {
        background: transparent;
    }

    DataTable {
        height: 1fr;
        border: solid $primary;
        background: transparent;
        scrollbar-background: transparent;
        scrollbar-background-hover: transparent;
        scrollbar-background-active: transparent;
        scrollbar-color: magenta;
        scrollbar-color-hover: magenta 80%;
        scrollbar-color-active: magenta 60%;
        scrollbar-size: 1 2;
        scrollbar-corner-color: transparent;
    }

    DataTable > .datatable--header {
        background: transparent;
    }

    DataTable > .datatable--cursor {
        background: $primary;
    }

    Static {
        background: transparent;
    }
    """

    def compose(self) -> ComposeResult:
        yield Sidebar()
        with Container(id="main-container"):
            with Vertical():
                yield TextArea(placeholder="Digite algo aqui...", id="input_text")
                yield DataTable(id="data_table", cursor_type="cell", header_height=3)

    def _make_header(self, col_type: str, col_name: str) -> Text:
        """Cria header com nome em cima, tipo embaixo e linha separadora."""
        t = Text()
        t.append(col_name, style="bold")
        t.append("\n")
        t.append(col_type, style="dim italic")
        t.append("\n")
        t.append("─" * 16, style="dim")
        return t

    def on_mount(self) -> None:
        """Configurar a tabela quando a app montar"""
        random.seed(42)
        table = self.query_one("#data_table", DataTable)

        columns = [
            ("INT", "id"),
            ("VARCHAR", "first_name"),
            ("VARCHAR", "last_name"),
            ("VARCHAR", "email"),
            ("VARCHAR", "phone"),
            ("DATE", "birth_date"),
            ("INT", "age"),
            ("VARCHAR", "gender"),
            ("VARCHAR", "address"),
            ("VARCHAR", "city"),
            ("VARCHAR", "state"),
            ("VARCHAR", "country"),
            ("VARCHAR", "zip_code"),
            ("VARCHAR", "company"),
            ("VARCHAR", "department"),
            ("VARCHAR", "job_title"),
            ("DECIMAL", "salary"),
            ("DATE", "hire_date"),
            ("BOOLEAN", "is_active"),
            ("VARCHAR", "username"),
            ("VARCHAR", "status"),
            ("INT", "login_count"),
            ("TIMESTAMP", "last_login"),
            ("FLOAT", "rating"),
            ("INT", "orders_count"),
            ("DECIMAL", "total_spent"),
            ("VARCHAR", "plan"),
            ("BOOLEAN", "email_verified"),
            ("VARCHAR", "timezone"),
            ("VARCHAR", "language"),
            ("TEXT", "notes"),
            ("INT", "referral_count"),
            ("DECIMAL", "credit_balance"),
            ("TIMESTAMP", "created_at"),
            ("TIMESTAMP", "updated_at"),
        ]

        for col_type, col_name in columns:
            table.add_column(self._make_header(col_type, col_name), width=16)

        first_names = ["João", "Maria", "Pedro", "Ana", "Carlos", "Lucia", "Rafael", "Fernanda", "Bruno", "Julia", "Lucas", "Camila", "Diego", "Beatriz", "Thiago", "Larissa", "Gustavo", "Amanda", "Mateus", "Isabela"]
        last_names = ["Silva", "Santos", "Costa", "Lima", "Oliveira", "Souza", "Pereira", "Almeida", "Ferreira", "Rodrigues", "Barros", "Ribeiro", "Martins", "Carvalho", "Gomes"]
        cities = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Porto Alegre", "Salvador", "Brasília", "Recife", "Fortaleza", "Manaus"]
        states = ["SP", "RJ", "MG", "PR", "RS", "BA", "DF", "PE", "CE", "AM"]
        countries = ["Brasil"]
        companies = ["TechCorp", "DataSoft", "CloudBase", "NetSys", "InfoPrime", "ByteWorks", "CodeLab", "DevHub", "LogiTech", "SmartApp"]
        departments = ["Engenharia", "Marketing", "Vendas", "RH", "Financeiro", "Suporte", "Produto", "Design", "QA", "DevOps"]
        job_titles = ["Analista", "Desenvolvedor", "Gerente", "Diretor", "Estagiário", "Coordenador", "Especialista", "Consultor", "Arquiteto", "Tech Lead"]
        statuses = ["active", "inactive", "suspended", "pending"]
        plans = ["free", "basic", "pro", "enterprise"]
        timezones = ["America/Sao_Paulo", "America/Manaus", "America/Bahia", "America/Recife"]
        languages = ["pt-BR", "en-US", "es-ES"]
        notes_list = ["Cliente VIP", "Novo cadastro", "Pendente revisão", "Sem observações", "Conta migrada", "Suporte prioritário"]

        base_date = datetime(2020, 1, 1)

        for i in range(1, 401):
            fname = random.choice(first_names)
            lname = random.choice(last_names)
            city_idx = random.randint(0, len(cities) - 1)
            birth = base_date - timedelta(days=random.randint(7000, 20000))
            hire = base_date + timedelta(days=random.randint(0, 1800))
            age = (datetime.now() - birth).days // 365
            salary = round(random.uniform(2500, 35000), 2)
            last_login = datetime.now() - timedelta(hours=random.randint(1, 2000))
            created = base_date + timedelta(days=random.randint(0, 1500))
            updated = created + timedelta(days=random.randint(0, 500))

            table.add_row(
                str(i),
                fname,
                lname,
                f"{fname.lower()}.{lname.lower()}@email.com",
                f"+55 11 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}",
                birth.strftime("%Y-%m-%d"),
                str(age),
                random.choice(["M", "F"]),
                f"Rua {random.randint(1, 999)}, {random.randint(1, 500)}",
                cities[city_idx],
                states[city_idx],
                random.choice(countries),
                f"{random.randint(10000, 99999)}-{random.randint(100, 999)}",
                random.choice(companies),
                random.choice(departments),
                random.choice(job_titles),
                f"{salary:,.2f}",
                hire.strftime("%Y-%m-%d"),
                random.choice(["true", "false"]),
                f"{fname.lower()}.{lname.lower()}{random.randint(1, 99)}",
                random.choice(statuses),
                str(random.randint(0, 500)),
                last_login.strftime("%Y-%m-%d %H:%M"),
                f"{random.uniform(1, 5):.1f}",
                str(random.randint(0, 150)),
                f"{random.uniform(100, 50000):.2f}",
                random.choice(plans),
                random.choice(["true", "false"]),
                random.choice(timezones),
                random.choice(languages),
                random.choice(notes_list),
                str(random.randint(0, 20)),
                f"{random.uniform(0, 5000):.2f}",
                created.strftime("%Y-%m-%d %H:%M"),
                updated.strftime("%Y-%m-%d %H:%M"),
            )


if __name__ == "__main__":
    app = TextualApp(ansi_color=True)
    app.run()
