import click

from dataci.models import Dataset


@click.group(name='dataset')
def dataset():
    """DataCI dataset management"""
    pass


@dataset.command()
@click.option('-n', '--name', type=str, required=True, help='Dataset name')
@click.argument('targets', type=str)
def save(name, targets):
    """
    Save a dataset to the workspace.
    1. Generate dataset schema based on dataset files and perform schema checking
    2. Generate dataset version
    3. copy content of dataset files to workspace data storage and save dataset metadata to database
    
    Command syntax:
        -n/--name: dataset name
        targets: path to dataset base directory
    """
    dataset = Dataset(
        name=name, dataset_files=targets,
    )
    dataset.save()


@dataset.command()
@click.argument('targets', type=str)
def publish(targets):
    """
    Publish a saved dataset.

    Command syntax:
        targets: dataset identifier, including workspace, dataset name and version.
    """
    datasets = Dataset.find(dataset_identifier=targets, tree_view=False, all=True)
    if len(datasets) == 0:
        raise ValueError(f'No dataset found for {targets}')
    if len(datasets) > 1:
        raise ValueError(
            f'Dataset identifier {targets} is ambiguous, '
            f'please specify one of the following using longer version ID:\n'
            "\n".join([dataset.identifier for dataset in datasets]),
        )
    dataset = datasets[0]
    dataset.publish()


@dataset.command()
@click.argument('targets', type=str, required=False)
@click.option('-a', '--all', is_flag=True, default=False, help='List all datasets, including unpublished ones.')
def ls(targets, all):
    """
    List all datasets or a specific dataset.
    """
    dataset_version_dict = Dataset.find(dataset_identifier=targets, all=all)

    for dataset_name, version_dict in dataset_version_dict.items():
        print(dataset_name)
        if len(version_dict) > 0:
            print(
                f'|  Version\tYield pipeline\tParent dataset\tSize\tCreate time'
            )
        for version, dataset in version_dict.items():
            print(
                f'|- {version[:7]}\t{dataset.yield_pipeline or "N.A."}\t\t{dataset.parent_dataset or "N.A."}\t\t'
                f'{dataset.size or "N.A."}\t{dataset.create_date.strftime("%Y-%m-%d %H:%M:%S")}'
            )
