// Default parameter input
params.out = "myfile.fastq.gz"
params.biodrive_id = "250217-123456"
params.database = "neoref16s_v2"
params.seq_id = "240213_VL00257_65_AAFCMTMM5.NS0065"
params.mapping_file = "mapping_customer_P085_183746.1_341F-806R.json"
params.cutoff = "1"
params.sequencing_tech = "paired-end"

// BLOB CHANNEL

def year = ""

if (params.seq_id.contains(".NS") || params.seq_id.contains(".SEQ")) {
    year = "20" + params.seq_id.take(2)
} else if (params.seq_id.contains(".ONT")) {
    year = params.seq_id.take(4)
} else {
    throw new IllegalArgumentException(
        "The input string '$params.seq_id' does not match the expected pattern for year extraction."
    )
}

mapping_blob = "fq/$year/$params.seq_id/maps/$params.mapping_file"
mapping_file_ch = Channel.of(mapping_blob)

// CONFIG JSON VARS

batchdir = "\$HOME/storage/results/dmd/${params.biodrive_id}.json"
config_json = "input_dmd_${params.biodrive_id}.json"


process downloadMappingFile{
    publishDir 'results'
    containerOptions '-v /home/neoprospecta/.credentials/biodrive-neopct-c528dcdd212e.json:/home/neoprospecta/.credentials/biodrive-neopct-c528dcdd212e.json'

    input:
    val blob

    output:
    file "$params.mapping_file"

    script:
    """
    #!/usr/bin/env python

    from shared.gcs_manager import GCSManager
    from pathlib import Path
    
    gcs_manager = GCSManager(bucket="fastq_cold")
    out_path = Path("$params.mapping_file")
    gcs_manager.download_blob("$mapping_blob", out_path=out_path)
    """
}

process createConfigJSON{
    publishDir 'results'

    input:
    file mapping_file

    output:
    file "$config_json"

    script:
    """
    #!/usr/bin/env python

    import json

    with open("$params.mapping_file") as mapping_file, open("$config_json", "w") as config_json:
        mapping_content = json.loads(mapping_file.read())
        config_content = {
            "CustumerList": [
                "P076",
                "NEOPCT"
            ],
            "batchdir": "$batchdir",
            "configfile": "$batchdir/$config_json",
            "filesname": {
            },
            "loggin": {
            },
            "neodb": {
                "model": "?",
                "path": "?",
                "schm": "$params.database"
            },
            "pipeline": {
                "kind": "dmd",
                "parameters": {
                    "OTUValidation": {
                        "ClusterValidation": {
                            "ClustFreqCutOff": "5",
                            "ClusterCovTax": "0.00"
                        },
                        "SampleValidation": {
                            "SampleCovCutoff": 300,
                            "TaxCovCutoff": 0.01
                        }
                    },
                    "ReadsConfig": "PairEnd",
                    "SeqFilter": {
                        "ExpectedError": "NeoSeqFilter"
                    },
                    "adpters": {
                        "name": "adpters",
                        "path": "/home/ubuntu/storage/neorefs/adapters_illumina.fasta"
                    },
                    "blast": {
                        "-evalue": "1e-5",
                        "-max_target_seqs": "20",
                        "-num_threads": "8",
                        "-outfmt": "'7 qseqid sseqid staxids pident qcovhsp length mismatch gaps qstart qend sstart send evalue bitscore'",
                        "-perc_identity": "95",
                        "-qcov_hsp_perc": "80",
                        "-word_size": "50"
                    },
                    "blastQC": {
                        "-evalue": "10",
                        "-max_target_seqs": "20",
                        "-num_threads": "8",
                        "-outfmt": "7",
                        "-perc_identity": "50",
                        "-qcov_hsp_perc": "30",
                        "-word_size": "7"
                    },
                    "norm": False,
                    "parse_blast": {
                        "identity": 99
                    }
                },
                "routine": "dmd4batch"
            }
        }
        main_content = {
            "configs": config_content,
            "id": "$params.biodrive_id",
            "libraries": mapping_content["libraries"]
        }
        jsonified_content = json.dumps(main_content, indent=4)
        config_json.write(jsonified_content)
    """
}

process getFASTQsBlobs{
    publishDir 'results'

    input:
    path mapping_file

    output:
    path "fastq_blobs.txt"

    script:
    """
    python $projectDir/src/get_fastqs_blobs/get_fastqs_blobs.py $mapping_file $year $params.seq_id 
    """
}

process downloadFASTQs {
    publishDir 'results/fastq'
    containerOptions '-v /home/neoprospecta/.credentials/biodrive-neopct-c528dcdd212e.json:/home/neoprospecta/.credentials/biodrive-neopct-c528dcdd212e.json'
    
    input:
    val blob_file

    output:
    path "*.fastq.gz"
    
    script:
    """
    python /app/src/download_fastqs/download_fastqs.py $blob_file
    """
}

process decompressFASTQs{
    publishDir 'results/decompressed'
    
    input:
    path comp_fastq_file

    output:
    path "*.fastq"
    
    script:
    """
    gunzip --force ${comp_fastq_file}
    """
}


process FASTQ2FASTA{
    publishDir 'results/fasta'
    
    input:
    path fastq_file

    output:
    path "*.fasta"
    
    script:
    """
    python $projectDir/src/fastq_to_fasta/fastq_to_fasta.py ${fastq_file}
    """
}


process trimFASTA {
    publishDir 'results/trimmed'
    
    input:
    path fasta_file

    output:
    path "trimmed_${fasta_file}"
    
    script:
    """
    python $projectDir/src/trimming/trim_fasta.py $fasta_file
    """
}


process standarizeSequenceSize {
    publishDir 'results/trimmed'
    
    input:
    path fasta_file

    output:
    path "std_${fasta_file}"
    
    script:
    """
    python $projectDir/src/standarize_size/standarize_size.py $fasta_file
    """
}

process createDeblurInput {
    publishDir 'results/trimmed'
    
    input:
    path fasta_file

    output:
    path "deblur_${fasta_file}"
    
    script:
    """
    python $projectDir/src/create_deblur_input/create_deblur_input.py $fasta_file $params.cutoff
    """
}

process denoiseWithDeblur {
    publishDir "results/cleaned"

    input:
    path fasta_file

    output:
    path "all.seqs.fa"

    script:
    """
    deblur workflow --output-dir ./ --seqs-fp ${fasta_file} -t -1 -w -a 0 --min-reads 1
    """
}

process clusteringWithVsearch {
    publishDir "results/clustered"

    input:
    path fasta_file

    output:
    path "*"

    script:
    """
    vsearch --uchime3_denovo ${fasta_file} --nonchimeras oligotypes_${fasta_file.baseName}.fasta --chimeras chimeras_${fasta_file.baseName}.fasta
    """
}


// Workflow block
workflow {
    mapping_file = downloadMappingFile(mapping_file_ch)
    createConfigJSON(mapping_file)
    getFASTQsBlobs(mapping_file)
    blobs = getFASTQsBlobs.out.splitText().map{v -> v.trim()}

    // QC block
    fastqs = downloadFASTQs(blobs)
    decomp_fastqs = decompressFASTQs(fastqs)
    decomp_fastqs.fromFilePairs("*R{1,2}*.fastq")
    if (params.sequencing_tech == "paired-end"){
        merged_fastq = ""
    }
    // fastas = FASTQ2FASTA(decomp_fastqs)
    // trimmed_fastas = trimFASTA(fastas)
    // standarized_fastas = standarizeSequenceSize(trimmed_fastas)
    // deblur_input = createDeblurInput(standarized_fastas)
    // denoised_fastas = denoiseWithDeblur(deblur_input)
    // clustered_fastas = clusteringWithVsearch(denoised_fastas)
}