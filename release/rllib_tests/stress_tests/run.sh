#!/usr/bin/env bash

ray_version="" 
commit=""
ray_branch=""

for i in "$@"
do
echo "$i"
case "$i" in
    --ray-version=*)
    ray_version="${i#*=}"

    ;;
    --commit=*)
    commit="${i#*=}"
    ;;
    --ray-branch=*)
    ray_branch="${i#*=}"
    ;;
    --workload=*)
    ;;
    --help)
    usage
    exit
    ;;
    *)
    echo "unknown arg, $i"
    exit 1
    ;;
esac
done

if [[ $ray_version == "" || $commit == "" || $ray_branch == "" ]]
then
    echo "Provide --ray-version, --commit, and --ray-branch"
    exit 1
fi

echo "version: $ray_version"
echo "commit: $commit"
echo "branch: $ray_branch"
echo "workload: ignored"

wheel="https://s3-us-west-2.amazonaws.com/ray-wheels/$ray_branch/$commit/ray-$ray_version-cp37-cp37m-manylinux2014_x86_64.whl"

conda uninstall -y terminado
pip install -U pip
pip install -U "$wheel"
pip install "ray[rllib]" "ray[debug]"
pip install terminado
pip install boto3==1.4.8 cython==0.29.0

python3 wait_cluster.py

rllib train -f atari_impala_xlarge.yaml --ray-address=auto --queue-trials
