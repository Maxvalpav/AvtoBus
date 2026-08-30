using AvtoBus.Scheduling;
using Xunit;

namespace AvtoBus.Tests.Scheduling;

public class LeaderElectionTests
{
    [Fact]
    public async Task InMemory_election_runs_acquire_renew_release_cycle()
    {
        var election = new InMemoryLeaderElection();

        Assert.False(election.IsLeader("r"));

        Assert.True(await election.TryAcquireAsync("r", TimeSpan.FromSeconds(30)));
        Assert.True(election.IsLeader("r"));
        Assert.True(await election.RenewAsync("r", TimeSpan.FromSeconds(30)));

        await election.ReleaseAsync("r");
        Assert.False(election.IsLeader("r"));
    }

    [Fact]
    public async Task InMemory_acquire_stays_leader_for_same_resource()
    {
        var election = new InMemoryLeaderElection();
        await election.TryAcquireAsync("r", TimeSpan.FromSeconds(30));
        Assert.True(await election.TryAcquireAsync("r", TimeSpan.FromSeconds(30)));
    }

    [Fact]
    public async Task Distinct_resources_do_not_collide()
    {
        var election = new InMemoryLeaderElection();
        Assert.True(await election.TryAcquireAsync("a", TimeSpan.FromSeconds(30)));
        Assert.True(await election.TryAcquireAsync("b", TimeSpan.FromSeconds(30)));
        Assert.True(election.IsLeader("a"));
        Assert.True(election.IsLeader("b"));
    }
}
