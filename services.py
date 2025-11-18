import boto3
import json
from collections import defaultdict

def get_all_services_using_resource_explorer(region='us-east-1'):
    # Initialize Resource Explorer client
    client = boto3.client('resource-explorer-2', region_name=region)
    
    services = defaultdict(int)
    resources = []
    
    try:
        # Search for all resources using wildcard
        paginator = client.get_paginator('search')
        page_iterator = paginator.paginate(
            QueryString='*'  # Search for all resources
        )
        
        print("Searching for resources across all AWS services...\n")
        
        for page in page_iterator:
            for resource in page.get('Resources', []):
                resource_type = resource.get('ResourceType', 'Unknown')
                service = resource_type.split(':')[0] if ':' in resource_type else resource_type
                
                services[service] += 1
                resources.append({
                    'ResourceType': resource_type,
                    'ARN': resource.get('Arn', ''),
                    'Region': resource.get('Region', ''),
                    'Service': resource.get('Service', '')
                })
        
        # Display results
        print(f"Total unique services found: {len(services)}\n")
        print("Services and resource counts:")
        print("-" * 50)
        
        for service, count in sorted(services.items()):
            print(f"{service}: {count} resources")
        
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


def list_resource_explorer_indexes():
    """List all Resource Explorer indexes across regions."""
    
    print("Checking for Resource Explorer indexes...\n")
    
    # Get all regions
    ec2 = boto3.client('ec2', region_name='us-east-1')
    regions = [region['RegionName'] for region in ec2.describe_regions()['Regions']]
    
    indexes = []
    
    for region in regions:
        try:
            client = boto3.client('resource-explorer-2', region_name=region)
            response = client.list_indexes()
            
            if response.get('Indexes'):
                for index in response['Indexes']:
                    indexes.append({
                        'Region': index['Region'],
                        'Type': index['Type'],
                        'ARN': index['Arn']
                    })
                    print(f"Found index in {region}: {index['Type']}")
        except:
            continue
    
    return indexes


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
            resources.extend(page.get('Resources', []))
        
        print(f"\nFound {len(resources)} resources for service: {service_type}")
        
        for resource in resources:
            print(f"  - {resource.get('ResourceType')}: {resource.get('Arn')}")
        
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
    
    indexes = list_resource_explorer_indexes()
    
    if not indexes:
        print("\nNo Resource Explorer indexes found.")
        print("To use Resource Explorer, you need to:")
        print("1. Create an aggregator index in one region")
        print("2. Create local indexes in other regions (optional)")
        print("\nSee: https://docs.aws.amazon.com/resource-explorer/latest/userguide/")
    else:
        print(f"\nFound {len(indexes)} Resource Explorer index(es)")
        print("\nFetching all services...\n")
        
        # Use the first aggregator index found, or fall back to first index
        aggregator_region = None
        for idx in indexes:
            if idx['Type'] == 'AGGREGATOR':
                aggregator_region = idx['Region']
                break
        
        if not aggregator_region:
            aggregator_region = indexes[0]['Region']
        
        print(f"Using index in region: {aggregator_region}\n")
        
        # Get all services
        result = get_all_services_using_resource_explorer(region=aggregator_region)
        
        if result:
            print(f"\n{'=' * 60}")
            print(f"Summary: {result['total_resources']} total resources across {result['total_services']} services")
            print(f"{'=' * 60}")