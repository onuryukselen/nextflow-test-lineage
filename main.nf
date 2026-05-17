#!/usr/bin/env nextflow
nextflow.enable.dsl=2

/*
 * Lineage test pipeline
 *
 * Demonstrates lineage capture: val params + file inputs/outputs with sizes.
 *
 * Workflow:
 *   GENERATE_READS  — creates a synthetic FASTQ file for each sample
 *   ALIGN           — "aligns" reads (counts lines, simulates work)
 *   SORT_BAM        — "sorts" alignment output
 *   QC_REPORT       — aggregates stats across all samples
 */

params.samples      = ['HG001', 'HG002']   // sample IDs
params.read_length  = 150                   // simulated read length
params.num_reads    = 500                   // reads per sample

process GENERATE_READS {
    tag "${sample_id}"
    memory '256 MB'
    cpus 1

    input:
    val(sample_id)

    output:
    tuple val(sample_id), path("${sample_id}.fastq")

    script:
    """
    python3 -c "
import random
random.seed(hash('${sample_id}'))
bases = 'ACGT'
qual  = 'I' * ${params.read_length}
with open('${sample_id}.fastq', 'w') as f:
    for i in range(${params.num_reads}):
        seq = ''.join(random.choices(bases, k=${params.read_length}))
        f.write(f'@${sample_id}_read_{i}\\n{seq}\\n+\\n{qual}\\n')
"
    echo "Generated ${params.num_reads} reads for ${sample_id}"
    """
}

process ALIGN {
    tag "${sample_id}"
    memory '512 MB'
    cpus 2

    input:
    tuple val(sample_id), path(reads)

    output:
    tuple val(sample_id), path("${sample_id}.bam")

    script:
    """
    # Simulate alignment: count reads and write a simple alignment file
    read_count=\$(grep -c '^@' ${reads} || true)
    echo "ALIGNED sample=${sample_id} reads=\${read_count} ref=hg38" > ${sample_id}.bam
    cat ${reads} >> ${sample_id}.bam
    echo "Aligned \${read_count} reads for ${sample_id}"
    """
}

process SORT_BAM {
    tag "${sample_id}"
    memory '512 MB'
    cpus 2

    input:
    tuple val(sample_id), path(bam)

    output:
    tuple val(sample_id), path("${sample_id}.sorted.bam")

    script:
    """
    # Simulate sort: reverse the file as a stand-in
    sort ${bam} > ${sample_id}.sorted.bam
    echo "Sorted BAM for ${sample_id}: \$(wc -l < ${sample_id}.sorted.bam) lines"
    """
}

process QC_REPORT {
    memory '256 MB'
    cpus 1

    input:
    val(all_samples)
    path(bams)

    output:
    path("qc_report.txt")

    script:
    def sample_list = all_samples.join(', ')
    """
    echo "QC Report" > qc_report.txt
    echo "Samples: ${sample_list}" >> qc_report.txt
    echo "Read length: ${params.read_length}" >> qc_report.txt
    echo "Reads per sample: ${params.num_reads}" >> qc_report.txt
    for bam in ${bams}; do
        echo "  \$bam: \$(wc -l < \$bam) lines" >> qc_report.txt
    done
    cat qc_report.txt
    """
}

workflow {
    samples_ch = Channel.fromList(params.samples)

    reads    = GENERATE_READS(samples_ch)
    aligned  = ALIGN(reads)
    sorted   = SORT_BAM(aligned)

    QC_REPORT(
        sorted.map { it[0] }.collect(),
        sorted.map { it[1] }.collect()
    )
}
