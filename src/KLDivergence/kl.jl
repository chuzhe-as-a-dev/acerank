function kl_divergence(p::Array{Float64}, q::Array{Float64})
    return sum(map(kl_sample_divergence, p, q))
end

function kl_sample_divergence(p::Float64, q::Float64)
    return -p * log(q / p)
end

function generate_distribution(a::Array{Float64})
    s = sum(a)
    return map(x -> x / s, a)
end

function generate_distribution!(a::Array{Float64})
    s = sum(a)
    a = map(x -> x / s, a)
    return a
end


