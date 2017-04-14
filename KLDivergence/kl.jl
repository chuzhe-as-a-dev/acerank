function kl_divergence(p::Array{Float64}, q::Array{Float64})
    return sum(map(kl_sample_divergence, p, q))
end

function kl_sample_divergence(p::Float64, q::Float64)
    return -p * log(q / p)
end

p = [0.1, 0.2, 0.3, 0.3, 0.1]
q = [0.2, 0.2, 0.2, 0.2, 0.2]

println(kl_divergence(p, q))