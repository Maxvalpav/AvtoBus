using AvtoBus.QuickStart.Contracts;
using Microsoft.EntityFrameworkCore;

namespace AvtoBus.QuickStart.Data;

public sealed class Order
{
    public Guid Id { get; set; }
    public string CustomerId { get; set; } = "";
    public List<OrderItem> Items { get; set; } = [];
    public decimal Total => Items.Sum(i => i.Qty * i.Price);
}

public sealed class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options) : base(options) { }

    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(e =>
        {
            e.HasKey(o => o.Id);
            e.OwnsMany(o => o.Items, item => item.Property(i => i.Sku).IsRequired());
        });
    }
}
