cd ../
echo "Initialize DataCI"
python dataci/command/init.py -f

echo "Download Sample Raw Data"
# saved at data/pairwise_raw/
mkdir -p data
rm -r data/*
cp -r dataset/multimodal_pairwise_v1 data/pairwise_raw/
