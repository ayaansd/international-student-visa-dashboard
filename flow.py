from prefect import flow

@flow
def my_first_cloud_flow():
    print("Hello, Prefect Cloud! 🚀")

if __name__ == "__main__":
    my_first_cloud_flow()
