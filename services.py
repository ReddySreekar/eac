import boto3
import json
from collections import defaultdict
import sys


def _get_region_from_arn(arn):
    """Extract region from an ARN when possible."""
    if not arn or not arn.startswith('arn:'):
        return ''
    parts = arn.split(':')
    # ARN format: arn:partition:service:region:account:resource
    if len(parts) > 4:
        return parts[3]
    return ''


def _get_resource_name(resource):
    """Try multiple strategies to get a human-friendly resource name from the
    Resource Explorer resource item. Falls back to parsing the ARN.
    """
    if not resource:
        return ''

    # Common direct fields
    for key in ('ResourceName', 'Title', 'Name', 'DisplayName'):
        val = resource.get(key)
        if val:
            return val

    # Try properties/attributes which may be a JSON string or dict
    props = resource.get('Properties') or resource.get('Attributes') or resource.get('Resource')
    pdata = None
    if isinstance(props, str):
        try:
            pdata = json.loads(props)
        except Exception:
            pdata = None
    elif isinstance(props, dict):
        pdata = props

    if pdata:
        for k in ('name', 'Name', 'title', 'Title', 'resourceName'):
            if k in pdata and pdata[k]:
                return pdata[k]

    # Fallback: extract last ARN segment
    arn = resource.get('Arn') or resource.get('ARN')
    if arn:
        # Resource identifiers often follow last '/' or ':'
        seg = arn.split('/')[-1]
        if seg and seg != arn:
            return seg
        seg = arn.split(':')[-1]
        return seg

    return ''


def _shorten(text, max_len=100):
    if not text:
        return ''
    s = str(text)
    if len(s) <= max_len:
        return s
    keep = (max_len - 3) // 2
    return s[:keep] + '...' + s[-keep:]


def _print_resources_table(resources, title=None):
    """Print a list of resources (dicts or raw items) in a neat table.

    Expected keys per resource dict: Service, ResourceType, Name, Region, ARN
    """
    if title:
        print(title)

    headers = ['Service', 'ResourceType', 'Name', 'Region', 'ARN']

    rows = []
    for r in resources:
        if isinstance(r, dict):
            service = r.get('Service') or ''
            rtype = r.get('ResourceType') or ''
            name = r.get('Name') or _get_resource_name(r)
            region = r.get('Region') or ''
            arn = r.get('ARN') or r.get('Arn') or ''
        else:
            service = r.get('Service') or ''
            rtype = r.get('ResourceType') or ''
            name = _get_resource_name(r)
            region = r.get('Region') or _get_region_from_arn(r.get('Arn', ''))
            arn = r.get('Arn') or ''

        rows.append([service, rtype, name, region, arn])

    # Determine column widths (cap ARN width)
    col_widths = []
    for i, h in enumerate(headers):
        maxw = len(h)
        for row in rows:
            cell = row[i] or ''
            l = len(str(cell))
            if l > maxw:
                maxw = l
        if headers[i] == 'ARN' and maxw > 120:
            maxw = 120
        col_widths.append(maxw)

    sep = ' | '
    header_line = sep.join(headers[i].ljust(col_widths[i]) for i in range(len(headers)))
    print(header_line)
    print('-' * len(header_line))

    for row in rows:
        out_cells = []
        for i, cell in enumerate(row):
            text = str(cell) if cell is not None else ''
            if headers[i] == 'ARN':
                text = _shorten(text, max_len=col_widths[i])
            out_cells.append(text.ljust(col_widths[i]))
        print(sep.join(out_cells))

    print(f"\nTotal resources: {len(rows)}\n")


def list_resource_explorer_indexes():
    print("Checking for Resource Explorer indexes in requested region...\n")

    # Discover valid regions to validate user input
    try:
        ec2 = boto3.client('ec2')
        valid_regions = [r['RegionName'] for r in ec2.describe_regions()['Regions']]
    except Exception:
        print("Unable to retrieve AWS regions. Make sure your AWS credentials are configured.")
        return None, []

    while True:
        region = input("Enter AWS region to check (e.g. us-east-1): ").strip()
        if not region:
            print("Region input cannot be empty. Please enter a valid AWS region.")
            continue
        if region not in valid_regions:
            print(f"'{region}' is not a valid region. Example valid regions: {', '.join(valid_regions[:6])}...")
            retry = input("Try again? (y/n): ").strip().lower()
            if retry != 'y':
                return None, []
            continue
        break

    indexes = []
    try:
        client = boto3.client('resource-explorer-2', region_name=region)
        response = client.list_indexes()

        if response.get('Indexes'):
            for index in response['Indexes']:
                indexes.append({
                    'Region': index.get('Region', region),
                    'Type': index.get('Type', ''),
                    'ARN': index.get('Arn', '')
                })
                print(f"Found index in {region}: {index.get('Type', '')}")
        else:
            print(f"No Resource Explorer index found in region: {region}")
    except client.exceptions.ResourceNotFoundException:
        print("Resource Explorer not configured in this region.")
    except Exception as e:
        print(f"Error checking indexes in {region}: {e}")

    return region, indexes


def get_all_services_using_resource_explorer(region='us-east-1'):
    client = boto3.client('resource-explorer-2', region_name=region)

    services = defaultdict(int)
    resources = []

    try:
        paginator = client.get_paginator('search')
        page_iterator = paginator.paginate(
            QueryString='*'
        )
        print(f"Searching for resources in region '{region}'...\n")

        for page in page_iterator:
            for resource in page.get('Resources', []):
                resource_region = resource.get('Region') or _get_region_from_arn(resource.get('Arn', ''))

                if resource_region != region:
                    continue

                resource_type = resource.get('ResourceType', 'Unknown')
                service = resource_type.split(':')[0] if ':' in resource_type else resource_type

                services[service] += 1
                resources.append({
                    'ResourceType': resource_type,
                    'ARN': resource.get('Arn', ''),
                    'Region': resource_region,
                    'Service': resource.get('Service', ''),
                    'Name': _get_resource_name(resource)
                })

        print(f"Total unique services found: {len(services)}\n")
        print("Services and resource counts:")
        print("-" * 50)

        for service, count in sorted(services.items()):
            print(f"{service}: {count} resources")

        _print_resources_table(resources, title=f"Resources in region: {region}")

        return {
            'services': dict(services),
            'total_services': len(services),
            'total_resources': len(resources),
            'resources': resources
        }

    except client.exceptions.ResourceNotFoundException:
        print("Error: Resource Explorer is not set up in this region.")
        print("Please create an index using the AWS Console or CLI first.")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def search_by_service(service_type, region='us-east-1'):
    """
    Search for resources of a specific service type.
    
    Args:
        service_type: AWS service (e.g., 'ec2', 's3', 'lambda')
        region: AWS region where Resource Explorer is set up
    """

    client = boto3.client('resource-explorer-2', region_name=region)

    try:
        paginator = client.get_paginator('search')
        page_iterator = paginator.paginate(
            QueryString=f'service:{service_type}'
        )

        resources = []
        for page in page_iterator:
            for resource in page.get('Resources', []):
                resource_region = resource.get('Region') or _get_region_from_arn(resource.get('Arn', ''))
                if resource_region != region:
                    continue
                resources.append({
                    'ResourceType': resource.get('ResourceType', ''),
                    'ARN': resource.get('Arn', ''),
                    'Region': resource_region,
                    'Service': resource.get('Service', ''),
                    'Name': _get_resource_name(resource)
                })

        print(f"\nFound {len(resources)} resources for service: {service_type} in region {region}")

        # Print results as a table
        _print_resources_table(resources, title=f"Resources for service: {service_type} in {region}")

        return resources

    except Exception as e:
        print(f"Error searching for {service_type}: {str(e)}")
        return []


if __name__ == "__main__":
    # First, check if Resource Explorer is set up
    print("=" * 60)
    print("AWS Resource Explorer - Service Discovery")
    print("=" * 60)
    print()

    region, indexes = list_resource_explorer_indexes()

    if not region:
        print("\nNo region selected or unable to determine region.")
        print("To use Resource Explorer, you need to:")
        print("1. Create an aggregator index in the region you want to query")
        print("2. Create local indexes in other regions (optional)")
        print("\nSee: https://docs.aws.amazon.com/resource-explorer/latest/userguide/")
        sys.exit(1)

    if not indexes:
        print(f"\nNo Resource Explorer indexes found in region {region}.")
        print("Create an index in that region using the Console or CLI and try again.")
        sys.exit(1)

    print(f"\nFound {len(indexes)} Resource Explorer index(es) in region {region}")
    print("\nFetching all services...\n")

    # Use the user-selected region
    aggregator_region = region

    print(f"Using index in region: {aggregator_region}\n")

    # Get all services
    result = get_all_services_using_resource_explorer(region=aggregator_region)

    if result:
        print(f"\n{'=' * 60}")
        print(f"Summary: {result['total_resources']} total resources across {result['total_services']} services")
        print(f"{'=' * 60}")

    # Optionally search for a specific service
    while True:
        svc = input("\nEnter a service to search (e.g. 'ec2') or press Enter to exit: ").strip()
        if not svc:
            break
        search_by_service(svc, region=aggregator_region)
