from snow_control.load import get_plan_from_cache, write_out_sql_snowplan


def gen_queries(account: str, snowplan: dict) -> list:
    queries = []
    for role, config in snowplan["ROLES"].items():
        role_queries = [
            gen_grant_to_role(*priv, delta="-", grant_target=role)
            for priv in config["to_revoke"]
        ] + [
            gen_grant_to_role(*priv, delta="+", grant_target=role)
            for priv in config["to_grant"]
        ]
        queries += role_queries
    for user, config in snowplan["USERS"].items():
        user_queries = [
            gen_grant_to_user(*priv, delta="-", grant_target=user)
            for priv in config["to_revoke"]
        ] + [
            gen_grant_to_user(*priv, delta="+", grant_target=user)
            for priv in config["to_grant"]
        ]
        queries += user_queries

    executables = [" ".join(q) for q in queries]
    write_out_sql_snowplan(account, executables)
    return queries


def gen_grant_to_role(
    privilege: str, object_type: str, object_name: str, delta: str, grant_target: str
):
    modified_object_name = "" if object_type == "ACCOUNT" else object_name
    assert delta in ("+", "-")
    if delta == "+":
        return (
            "GRANT",
            privilege,
            "ON",
            object_type,
            modified_object_name,
            "TO ROLE",
            grant_target,
        )
    else:
        return (
            "REVOKE",
            privilege,
            "ON",
            object_type,
            modified_object_name,
            "FROM ROLE",
            grant_target,
        )


def gen_grant_to_user(
    privilege: str, object_type: str, role_name: str, delta: str, grant_target: str
):
    assert delta in ("+", "-")
    assert (
        privilege.strip() == "USAGE" and object_type.strip() == "ROLE"
    ), f'failed with privilege "{privilege}" and object_type "{object_type}"'
    grant_target = f'"{grant_target}"'  # Use quoted identifiers with users
    if delta == "+":
        return ("GRANT", "ROLE", role_name, "TO USER", grant_target)
    else:
        return ("REVOKE", "ROLE", role_name, "FROM USER", grant_target)
