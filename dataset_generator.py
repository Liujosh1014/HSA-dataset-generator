import json
import math
import random
from datetime import datetime, timedelta
from collections import deque


class MindMapGenerator:
    def __init__(
        self,
        seed: int = 42,
        project_id: str = "proj_synthetic_2k",
        owner_id: str = "test.user@absentminded.dev",
        root_label: str = "Machine Learning",
        total_nodes: int = 2000,
        max_depth: int = 6,
        min_children: int = 2,
        max_children: int = 5,
    ):
        self.rng = random.Random(seed)
        self.project_id = project_id
        self.owner_id = owner_id
        self.root_label = root_label
        self.total_nodes = total_nodes
        self.max_depth = max_depth
        self.min_children = min_children
        self.max_children = max_children

        self.nodes = []
        self.id_counter = 1

        self.topic_pool = {
            0: ["Large-Scale Software Project Management"],
            1: [
                "Requirement Management", "Architecture Design", "Team Collaboration",
                "Development Process", "Project Delivery"
            ],
            2: [
                "Requirement Analysis", "System Design", "Task Planning", "Version Control",
                "Testing Strategy", "Risk Management", "Deployment Management", "Maintenance Planning"
            ],
            3: [
                "User Stories", "Functional Specification", "Microservices Architecture", "Database Design",
                "Sprint Planning", "Code Review", "CI/CD Pipeline", "Integration Testing",
                "Bug Tracking", "Release Management", "Monitoring", "Documentation"
            ],
            4: [
                "Gather Requirements", "Define Scope", "Design API Contracts", "Split Services",
                "Assign Tasks", "Estimate Timeline", "Write Test Cases", "Setup CI Workflow",
                "Track Progress", "Manage Risks", "Prepare Release Notes", "Handle Incidents"
            ],
            5: [
                "Write Spec", "Create Ticket", "Implement Feature", "Fix Bug",
                "Review PR", "Run Tests", "Deploy Service", "Check Logs",
                "Update Docs", "Refactor Module", "Monitor Metrics", "Conduct Retrospective"
            ]
        }

        self.status_pool = ["TODO", "IN_PROGRESS", "DONE", "BLOCKED"]

    def next_id(self) -> str:
        nid = f"sdm_{self.id_counter:06d}"
        self.id_counter += 1
        return nid

    def pick_label(self, depth: int, sibling_index: int) -> str:
        if depth in self.topic_pool:
            base = self.rng.choice(self.topic_pool[depth])
        else:
            base = self.rng.choice(self.topic_pool[max(self.topic_pool.keys())])

        suffix = f"{depth}_{sibling_index}_{self.rng.randint(1, 999)}"
        return f"{base} {suffix}"

    def make_time_window(self, depth: int):
        now = datetime.now()
        start_offset = self.rng.randint(0, 30 + depth * 3)
        duration = self.rng.randint(3, 60)
        start = now + timedelta(days=start_offset)
        deadline = start + timedelta(days=duration)
        return start, deadline

    def build(self):
        root_id = self.next_id()
        start, deadline = self.make_time_window(0)

        root = {
            "id": root_id,
            "label": self.root_label,
            "description": None,
            "parent": None,
            "project": self.project_id,
            "owner_id": self.owner_id,
            "user_id": self.owner_id,
            "status": "TODO",
            "start": start.isoformat(sep=" ", timespec="seconds"),
            "deadline": deadline.isoformat(sep=" ", timespec="seconds"),
            "url": None,
            "depth": 0,
            "children": []
        }

        self.nodes.append(root)

        q = deque()
        q.append(root)

        while len(self.nodes) < self.total_nodes and q:
            parent = q.popleft()
            depth = parent["depth"]

            if depth >= self.max_depth:
                continue

            remaining_nodes = self.total_nodes - len(self.nodes)
            if remaining_nodes <= 0:
                break

            # 深度越深，平均 child 越少，讓樹比較自然
            depth_decay = max(1, self.max_children - depth // 2)
            actual_max_children = max(self.min_children, min(self.max_children, depth_decay))

            child_count = self.rng.randint(self.min_children, actual_max_children)
            child_count = min(child_count, remaining_nodes)

            for i in range(child_count):
                if len(self.nodes) >= self.total_nodes:
                    break

                child_id = self.next_id()
                c_start, c_deadline = self.make_time_window(depth + 1)

                child = {
                    "id": child_id,
                    "label": self.pick_label(depth + 1, i),
                    "description": None,
                    "parent": parent["id"],
                    "project": self.project_id,
                    "owner_id": self.owner_id,
                    "user_id": self.owner_id,
                    "status": self.rng.choices(
                        self.status_pool, weights=[0.55, 0.2, 0.15, 0.1], k=1
                    )[0],
                    "start": c_start.isoformat(sep=" ", timespec="seconds"),
                    "deadline": c_deadline.isoformat(sep=" ", timespec="seconds"),
                    "url": None,
                    "depth": depth + 1,
                    "children": []
                }

                parent["children"].append(child)
                self.nodes.append(child)
                q.append(child)

        return root

    def export_json(self, root, filepath: str):
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(root, f, ensure_ascii=False, indent=2)

    def export_flat_jsonl(self, filepath: str):
        with open(filepath, "w", encoding="utf-8") as f:
            for node in self.nodes:
                row = {k: v for k, v in node.items() if k != "children"}
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def sql_escape(self, value):
        if value is None:
            return "NULL"
        return "'" + str(value).replace("'", "''") + "'"

    def export_sql(self, filepath: str):
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"-- project: {self.project_id}\n")
            f.write(f"-- total nodes: {len(self.nodes)}\n\n")

            root_id = self.nodes[0]["id"]

            f.write(
                "INSERT INTO public.projects (id, name, owner_id, root_task)\n"
                f"VALUES ({self.sql_escape(self.project_id)}, "
                f"{self.sql_escape(self.root_label)}, "
                f"{self.sql_escape(self.owner_id)}, "
                f"{self.sql_escape(root_id)})\n"
                "ON CONFLICT (id) DO UPDATE SET root_task = EXCLUDED.root_task;\n\n"
            )

            f.write(
                "INSERT INTO public.project_participants (project_id, participants)\n"
                f"VALUES ({self.sql_escape(self.project_id)}, {self.sql_escape(self.owner_id)})\n"
                "ON CONFLICT DO NOTHING;\n\n"
            )

            chunk_size = 1000
            flat_nodes = [{k: v for k, v in n.items() if k != "children"} for n in self.nodes]

            for i in range(0, len(flat_nodes), chunk_size):
                chunk = flat_nodes[i:i + chunk_size]
                f.write(
                    "INSERT INTO public.tasks "
                    "(id, deadline, description, label, start, owner_id, parent, project, status, url, user_id)\nVALUES\n"
                )
                rows = []
                for node in chunk:
                    row = "(" + ", ".join([
                        self.sql_escape(node["id"]),
                        self.sql_escape(node["deadline"]),
                        self.sql_escape(node["description"]),
                        self.sql_escape(node["label"]),
                        self.sql_escape(node["start"]),
                        self.sql_escape(node["owner_id"]),
                        self.sql_escape(node["parent"]),
                        self.sql_escape(node["project"]),
                        self.sql_escape(node["status"]),
                        self.sql_escape(node["url"]),
                        self.sql_escape(node["user_id"]),
                    ]) + ")"
                    rows.append(row)

                f.write(",\n".join(rows))
                f.write("\nON CONFLICT (id) DO NOTHING;\n\n")


if __name__ == "__main__":
    gen = MindMapGenerator(
        seed=42,
        project_id="proj_Large_Scale_Software_Project_Management_2k",
        owner_id="test.user@absentminded.dev",
        root_label="Large-Scale Software Project Management",
        total_nodes=2000,
        max_depth=6,
        min_children=2,
        max_children=7,
    )

    root = gen.build()
    gen.export_json(root, "sdm_mindmap_2k_tree.json")
    gen.export_flat_jsonl("sdm_mindmap_2k_flat.jsonl")
    gen.export_sql("sdm_mindmap_2k.sql")

    print(f"Generated {len(gen.nodes)} nodes")
    print("Files:")
    print("- sdm_mindmap_2k_tree.json")
    print("- sdm_mindmap_2k_flat.jsonl")
    print("- sdm_mindmap_2k.sql")